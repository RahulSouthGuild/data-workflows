import os
import sys
import json
import gzip
import time
import asyncio
import pymysql
import aiofiles
import tracemalloc
import polars as pl
import gc
import requests
import tempfile
import uuid
from azure.storage.blob.aio import BlobServiceClient
from datetime import date
from pathlib import Path
from tqdm import tqdm
from opentelemetry import trace

tracemalloc.start()

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent  # Go up 4 levels to reach project root
sys.path.append(str(PROJECT_ROOT))

from utils.pipeline_config import Config, DAILY_FIS_INCREMENTAL_SERVICE_NAME, LOG_SEPARATOR
from utils.logging_utils import get_pipeline_logger, with_status_tracking
from core.transformers.transformation_engine import (
    validate_and_transform_dataframe,
    get_table_name_from_file,
)

# Import constants from Config for backward compatibility
FACT_CHUNK_SIZE = Config.FACT_CHUNK_SIZE
FACT_DELETE_CHUNK_SIZE = Config.FACT_DELETE_CHUNK_SIZE
MAX_CONCURRENT_DELETIONS = Config.MAX_CONCURRENT_DELETIONS
MAX_CONCURRENT_INSERTS = Config.MAX_CONCURRENT_INSERTS
LOCK_TIMEOUT = Config.LOCK_TIMEOUT
LOCK_FILE_PATH = Config.LOCK_FILE_PATH

FOLDERS = [
    "Incremental/FactInvoiceSecondary/LatestData/",
    "Incremental/FactInvoiceSecondary_107_112/LatestData/",
]

# StarRocks connection configuration
STARROCKS_CONFIG = {
    "host": Config.STARROCKS_HOST,
    "port": Config.STARROCKS_PORT,
    "http_port": Config.STARROCKS_HTTP_PORT,
    "user": Config.STARROCKS_USER,
    "password": Config.STARROCKS_PASSWORD,
    "database": Config.STARROCKS_DATABASE,
}

# Azure configuration
AZURE_CONFIG = Config.get_azure_config()


def get_starrocks_connection():
    """Create StarRocks MySQL connection"""
    return pymysql.connect(
        host=STARROCKS_CONFIG["host"],
        port=STARROCKS_CONFIG["port"],
        user=STARROCKS_CONFIG["user"],
        password=STARROCKS_CONFIG["password"],
        database=STARROCKS_CONFIG["database"],
        charset="utf8mb4",
        autocommit=True,
    )


def stream_load_csv(table_name, csv_file_path, chunk_id=None, columns=None):
    """Load CSV data into StarRocks using Stream Load API"""
    url = f"http://{STARROCKS_CONFIG['host']}:{STARROCKS_CONFIG['http_port']}/api/{STARROCKS_CONFIG['database']}/{table_name}/_stream_load"

    # Generate unique label: timestamp_chunk_uuid
    # This ensures no label duplication even with fast consecutive requests
    unique_id = str(uuid.uuid4())[:8]  # Use first 8 chars of UUID
    headers = {
        "label": f"{table_name}_{int(time.time())}_{chunk_id if chunk_id else ''}_{unique_id}",
        "column_separator": "\x01",
        "format": "CSV",
        "max_filter_ratio": str(Config.MAX_ERROR_RATIO),
        "strict_mode": "false",
        "timezone": "Asia/Shanghai",
        "Expect": "100-continue",
    }

    if columns:
        headers["columns"] = ",".join(columns)

    auth = (STARROCKS_CONFIG["user"], STARROCKS_CONFIG["password"])

    try:
        with open(csv_file_path, "rb") as f:
            file_data = f.read()

        response = requests.put(
            url, headers=headers, data=file_data, auth=auth, timeout=Config.STREAM_LOAD_TIMEOUT
        )

        result = response.json()
        return result.get("Status") == "Success", result
    except Exception as e:
        return False, {"Message": str(e)}


def acquire_lock(task_logger):
    """Acquire file-based lock with timeout"""
    for attempt in range(5):
        if LOCK_FILE_PATH.exists():
            age = time.time() - LOCK_FILE_PATH.stat().st_mtime
            if age > LOCK_TIMEOUT:
                task_logger.warning(f"🔓 Stale lock detected (age: {age}s), removing...")
                LOCK_FILE_PATH.unlink()
            else:
                task_logger.warning(f"🔒 Lock exists (age: {age}s), waiting 30s...")
                time.sleep(30)
        else:
            LOCK_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
            LOCK_FILE_PATH.write_text(str(time.time()))
            return True

    raise RuntimeError("❌ Could not acquire lock after 2.5 minutes")


def release_lock(task_logger):
    """Release file-based lock"""
    try:
        if LOCK_FILE_PATH.exists():
            LOCK_FILE_PATH.unlink()
            task_logger.info("🔓 Lock released")
    except Exception as e:
        task_logger.error(f"❌ Error releasing lock: {e}")


def clean_file_name(filename):
    """Clean file name by replacing special characters"""
    import re

    return re.sub(r"[^\w\-]", "_", filename)


def get_table_name_from_stem(stem, task_logger):
    """Extract table name from file stem"""
    # DimHierarchy -> dim_hierarchy
    # DimDealer_MS -> dim_dealer
    # FactInvoiceSecondary -> fact_invoice_secondary
    name = stem.split("_")[0]  # Get first part before timestamp
    # Convert CamelCase to snake_case
    import re

    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def get_table_count(table_name="fact_invoice_secondary"):
    """Get COUNT(*) from table. Returns None on error."""
    try:
        conn = get_starrocks_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        return None


async def delete_chunk(records, record_bar, task_logger, attempt=1):
    """Delete records from StarRocks using raw SQL"""
    if not records:
        return 0

    try:
        start_time = time.time()

        # Generate composite IDs for deletion
        composite_ids = []
        for r in records:
            composite_id = (
                (str(r["invoice_date"]) if r.get("invoice_date") is not None else "NULL")
                + "_"
                + (str(r["customer_code"]) if r.get("customer_code") is not None else "NULL")
                + "_"
                + (str(r["invoice_no"]) if r.get("invoice_no") is not None else "NULL")
            )
            composite_ids.append(composite_id)

        unique_composite_ids = list(set(composite_ids))

        if len(unique_composite_ids) != len(records):
            task_logger.warning(
                f"⚠️ Found {len(records) - len(unique_composite_ids)} duplicate records in input chunk"
            )

        task_logger.info(
            f"📊 DELETE CHUNK: Attempting to delete {len(unique_composite_ids)} unique composite IDs"
        )

        # Execute delete via MySQL connection
        # IMPORTANT: StarRocks has a limit of 10,000 expressions in IN clause
        # Split into batches of 5,000 to be safe
        BATCH_SIZE = 5000
        deleted_count = 0
        conn = get_starrocks_connection()

        try:
            with conn.cursor() as cursor:
                # Process IDs in batches
                for batch_start in range(0, len(unique_composite_ids), BATCH_SIZE):
                    batch_end = min(batch_start + BATCH_SIZE, len(unique_composite_ids))
                    batch_ids = unique_composite_ids[batch_start:batch_end]

                    # Use composite ID for deletion - no record_type filter
                    # Incremental ingestion: delete ANY record with matching fis_sg_id_cc_in
                    placeholders = ", ".join(["%s"] * len(batch_ids))
                    query = f"""
                        DELETE FROM fact_invoice_secondary
                        WHERE fis_sg_id_cc_in IN ({placeholders})
                    """
                    cursor.execute(query, batch_ids)

                    task_logger.debug(
                        f"  Batch {batch_start//BATCH_SIZE + 1}: Executing DELETE "
                        f"({batch_start}-{batch_end}/{len(unique_composite_ids)})"
                    )

            # Return total records attempted (bulk_delete will calculate actual via COUNT diff)
            deleted_count = len(records)

        finally:
            conn.close()

        duration = time.time() - start_time
        record_bar.update(len(records))
        record_bar.set_postfix_str(f"Deleted {deleted_count} in {duration:.2f}s")

        task_logger.info(f"✅ DELETE CHUNK: Deleted {deleted_count} records in {duration:.2f}s")
        return deleted_count

    except Exception as e:
        task_logger.error(f"[Delete] Error: {e}")
        if attempt < Config.MAX_RETRIES:
            wait = Config.RETRY_DELAY**attempt
            task_logger.warning(
                f"[Delete] Retry attempt {attempt}/{Config.MAX_RETRIES} in {wait:.2f}s..."
            )
            await asyncio.sleep(wait)
            return await delete_chunk(records, record_bar, task_logger, attempt + 1)
        else:
            task_logger.error(f"[Delete] Maximum retries reached")
            raise


async def bulk_delete(df, task_logger):
    records = df.to_dicts()
    total_records = len(records)
    total_chunks = (total_records + FACT_DELETE_CHUNK_SIZE - 1) // FACT_DELETE_CHUNK_SIZE

    task_logger.info(f"🗑️  Starting bulk delete: {total_records:,} records in {total_chunks} chunks")

    # Get COUNT BEFORE deletion (single call using helper)
    count_before = get_table_count()
    if count_before is not None:
        task_logger.info(f"📊 Table count BEFORE deletion: {count_before:,} rows")
    else:
        task_logger.warning(f"⚠️ Could not get count before deletion")

    if total_records > 0:
        sample_record = records[0]
        task_logger.debug(
            f"   Sample: invoice_date={sample_record.get('invoice_date')}, "
            f"customer_code={sample_record.get('customer_code')}, "
            f"invoice_no={sample_record.get('invoice_no')}"
        )

    chunk_bar = tqdm(total=total_chunks, desc="Delete Chunks", ncols=80, position=0)
    record_bar = tqdm(total=total_records, desc="Delete Records", ncols=80, position=1)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DELETIONS)
    completed_chunks = 0

    async def delete_with_semaphore(chunk, chunk_idx):
        nonlocal completed_chunks
        async with semaphore:
            chunk_size = len(chunk)
            deleted_count = await delete_chunk(chunk, record_bar, task_logger)
            completed_chunks += 1
            chunk_bar.update(1)
            chunk_bar.set_postfix_str(f"{completed_chunks}/{total_chunks} completed")

            if deleted_count != chunk_size:
                diff = chunk_size - deleted_count
                task_logger.info(
                    f"📝 Chunk {completed_chunks}: Deleted {deleted_count:,}, {diff:,} new"
                )
            else:
                task_logger.info(f"✅ Chunk {completed_chunks}: Deleted all {deleted_count:,}")
            return deleted_count

    tasks = []
    for i, chunk_start in enumerate(range(0, total_records, FACT_DELETE_CHUNK_SIZE)):
        chunk = records[chunk_start : chunk_start + FACT_DELETE_CHUNK_SIZE]
        tasks.append(asyncio.create_task(delete_with_semaphore(chunk, i)))

    try:
        results = await asyncio.gather(*tasks)
        deleted = sum(results)
    except Exception as e:
        chunk_bar.close()
        record_bar.close()
        task_logger.error(f"❌ Bulk delete failed: {e}")
        raise

    chunk_bar.close()
    record_bar.close()

    # Get COUNT AFTER deletion (single call using helper)
    count_after = get_table_count()
    if count_after is not None:
        task_logger.info(f"📊 Table count AFTER deletion:  {count_after:,} rows")
        if count_before is not None:
            actual_deleted = count_before - count_after
            task_logger.info(f"📊 Actual deleted (count diff): {actual_deleted:,} rows")
    else:
        task_logger.warning(f"⚠️ Could not get count after deletion")

    if deleted != total_records:
        diff = total_records - deleted
        task_logger.info(
            f"🗑️  Delete complete: {deleted:,} deleted of {total_records:,} ({diff:,} new)"
        )
    else:
        task_logger.info(f"🗑️  Delete complete: All {deleted:,} records deleted")

    return deleted


async def insert_chunk(sem, chunk, bar, task_logger, db_column_order=None, attempt=1):
    """Insert chunk using Stream Load API"""
    async with sem:
        try:
            start_time = time.time()

            # Convert parquet chunk to CSV in temp file
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                csv_path = tmp.name

            # Write parquet to CSV with column separator
            chunk.write_csv(csv_path, separator="\x01", include_header=False)

            # Load via Stream Load with explicit column mapping
            success, result = stream_load_csv(
                "fact_invoice_secondary", csv_path, columns=db_column_order
            )

            # Clean up temp file
            Path(csv_path).unlink(missing_ok=True)

            if not success:
                raise Exception(f"Stream Load failed: {result.get('Message', 'Unknown error')}")

            loaded = result.get("NumberLoadedRows", 0)
            bar.update(chunk.height)
            bar.set_postfix_str(f"Inserted {loaded} in {time.time() - start_time:.2f}s")

            return loaded

        except Exception as e:
            task_logger.error(f"[Insert] Error: {e}")
            if attempt < Config.MAX_RETRIES:
                wait = Config.RETRY_DELAY**attempt
                task_logger.warning(
                    f"[Insert] Retry {attempt}/{Config.MAX_RETRIES} in {wait:.2f}s..."
                )
                await asyncio.sleep(wait)
                return await insert_chunk(
                    sem, chunk, bar, task_logger, db_column_order, attempt + 1
                )
            else:
                task_logger.error(f"[Insert] Maximum retries reached")
                raise


async def insert_data(df, task_logger):
    """Insert data using Stream Load API"""
    total_records = df.height

    if total_records == 0:
        task_logger.warning("⚠️ No records to insert")
        return 0

    # Note: invoice_composite_id is already generated during transform_dataframe()
    # via the centralized transformation engine from transformation_engine.py

    total_chunks = (total_records + FACT_CHUNK_SIZE - 1) // FACT_CHUNK_SIZE
    task_logger.info(f"⬆️  Starting bulk insert: {total_records:,} records in {total_chunks} chunks")

    if total_records > 0:
        sample_row = df.row(0, named=True)
        task_logger.debug(f"   Sample columns: {list(df.columns[:3])}")

    # Get database column order once and reuse for all chunks
    db_column_order = None
    try:
        conn = get_starrocks_connection()
        with conn.cursor() as cursor:
            cursor.execute("DESC fact_invoice_secondary")
            db_columns_raw = cursor.fetchall()
            db_column_order = [col[0] for col in db_columns_raw]
        conn.close()

        # Build ordered column list matching database table order
        df_columns = df.columns
        df_columns_lower = {col.lower(): col for col in df_columns}

        ordered_df_columns = []
        ordered_db_columns = []

        for db_col in db_column_order:
            db_col_lower = db_col.lower()
            if db_col_lower in df_columns_lower:
                ordered_df_columns.append(df_columns_lower[db_col_lower])
                ordered_db_columns.append(db_col)

        # Reorder dataframe to match database table order
        if ordered_df_columns:
            df = df.select(ordered_df_columns)
            db_column_order = ordered_db_columns
            task_logger.info(
                f"✓ Column mapping verified - {len(db_column_order)} columns will be loaded"
            )
    except Exception as e:
        task_logger.warning(f"⚠️ Could not verify column order: {e}, using as-is")
        db_column_order = list(df.columns)

    bar = tqdm(total=total_records, desc="Insert Records", ncols=80)
    sem = asyncio.Semaphore(MAX_CONCURRENT_INSERTS)
    tasks = []

    for chunk_idx, i in enumerate(range(0, df.height, FACT_CHUNK_SIZE)):
        chunk = df.slice(i, FACT_CHUNK_SIZE)
        if not chunk.is_empty():
            tasks.append(
                asyncio.create_task(insert_chunk(sem, chunk, bar, task_logger, db_column_order))
            )

    if not tasks:
        task_logger.error(f"❌ No insert tasks created! Records: {total_records}")
        bar.close()
        return 0

    try:
        task_logger.debug(f"   Executing {len(tasks)} insert tasks...")
        results = await asyncio.gather(*tasks)
        inserted = sum(results)
    except Exception as e:
        bar.close()
        task_logger.error(f"❌ Bulk insert failed: {e}")
        raise

    bar.close()

    # Get COUNT AFTER insertion (single call using helper)
    count_after_insert = get_table_count()
    if count_after_insert is not None:
        task_logger.info(f"📊 Table count AFTER insertion: {count_after_insert:,} rows")
    else:
        task_logger.warning(f"⚠️ Could not get count after insertion")

    if inserted != total_records:
        error_msg = f"❌ INSERT MISMATCH: Expected {total_records:,}, got {inserted:,}"
        task_logger.error(error_msg)
        raise ValueError(error_msg)

    task_logger.info(f"⬆️  Insert complete: {inserted:,}/{total_records:,} records")
    return inserted


async def process_file(filename, task_logger, main_logger=None):
    """Process FIS file with lock management"""
    try:
        start_time = time.time()
        task_logger.info(LOG_SEPARATOR)
        file_path = PROJECT_ROOT / "data" / "data_incremental" / "cleaned_parquets" / filename

        # Acquire file-based lock
        task_logger.info(f"🔒 Acquiring lock for FIS processing...")
        acquire_lock(task_logger)
        task_logger.info(f"✅ Lock acquired successfully")

        # Get total record count
        df_lazy = pl.scan_parquet(file_path)
        initial_record_count = df_lazy.select(pl.len()).collect().item()

        task_logger.info(f"📊 {filename}: Starting with {initial_record_count:,} records")
        task_logger.info(f"   File: {file_path}")
        task_logger.info(f"   Started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        task_logger.info(LOG_SEPARATOR)

        # Process in batches
        PROCESS_BATCH_SIZE = 100_000
        total_deleted = 0
        total_inserted = 0

        num_batches = (initial_record_count + PROCESS_BATCH_SIZE - 1) // PROCESS_BATCH_SIZE
        task_logger.info(f"📦 Processing {initial_record_count:,} records in {num_batches} batches")

        # 1. Load all batches
        batches = []
        for batch_idx in range(num_batches):
            offset = batch_idx * PROCESS_BATCH_SIZE
            batch_num = batch_idx + 1
            task_logger.info(f"📦 Loading batch {batch_num}/{num_batches}")
            df_batch = df_lazy.slice(offset, PROCESS_BATCH_SIZE).collect()
            batch_size = df_batch.height
            task_logger.info(f"   Loaded {batch_size:,} records")
            batches.append(df_batch)

        del df_lazy
        gc.collect()

        # 2. Delete all batches
        task_logger.info(f"🗑️  Starting DELETE for {num_batches} batches...")
        for batch_num, df_batch in enumerate(batches, 1):
            batch_size = df_batch.height
            task_logger.info(f"   📊 BATCH {batch_num}: Deleting {batch_size:,} records")
            deleted = await bulk_delete(df_batch, task_logger)
            total_deleted += deleted
            if deleted != batch_size:
                diff = batch_size - deleted
                task_logger.info(
                    f"📝 BATCH {batch_num}: Deleted {deleted:,}, will insert {batch_size:,} ({diff:,} net new)"
                )
            else:
                task_logger.info(f"✅ BATCH {batch_num}: Deleted all {deleted:,}")

        # 3. Insert all batches
        task_logger.info(f"⬆️  Starting INSERT for {num_batches} batches...")
        for batch_num, df_batch in enumerate(batches, 1):
            batch_size = df_batch.height
            task_logger.info(f"   📊 BATCH {batch_num}: Inserting {batch_size:,} records")
            inserted = await insert_data(df_batch, task_logger)
            total_inserted += inserted if inserted else 0
            del df_batch
            gc.collect()
            task_logger.info(f"   ✅ BATCH {batch_num}/{num_batches} complete")

        expected_inserted = initial_record_count

        # REVISED DATA INTEGRITY CHECK: Focus on what matters
        # 1. We should insert exactly what we read from the file
        if total_inserted != expected_inserted:
            error_msg = (
                f"❌ INSERT COUNT ERROR for {filename}: "
                f"Read {expected_inserted:,} records from file but inserted {total_inserted:,} records"
            )
            task_logger.error(LOG_SEPARATOR)
            task_logger.error(error_msg)
            task_logger.error(LOG_SEPARATOR)
            if main_logger:
                main_logger.error(error_msg)
            raise ValueError(error_msg)

        # 2. Log the net change (informational, not an error)
        net_change = total_inserted - total_deleted
        if net_change > 0:
            task_logger.info(f"📈 Net INCREASE: {net_change:,}")
        elif net_change < 0:
            task_logger.info(f"📉 Net DECREASE: {abs(net_change):,}")
        else:
            task_logger.info(f"➡️ Net UNCHANGED: Replaced {total_deleted:,}")

        # Get FINAL table count
        try:
            conn = get_starrocks_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM fact_invoice_secondary")
                final_table_count = cursor.fetchone()[0]
            conn.close()
            task_logger.info(f"📊 FINAL table count: {final_table_count:,} rows")
        except Exception as e:
            task_logger.warning(f"⚠️ Could not get final count: {e}")
            final_table_count = None

        # Log summary
        task_logger.info(LOG_SEPARATOR)
        task_logger.info("📊 Processing Summary:")
        processing_time = time.time() - start_time
        summary_lines = (
            f"✅ {filename} success in {processing_time:.2f}s\n"
            f"📊 Input records: {initial_record_count:,}\n"
            f"🗑️  Records deleted: {total_deleted:,}\n"
            f"⬆️  Records inserted: {total_inserted:,}\n"
            f"📦 Processing batches: {num_batches}"
        )
        if final_table_count is not None:
            summary_lines += f"\n📊 Final table count: {final_table_count:,} rows"
        task_logger.info(summary_lines)
        task_logger.info(LOG_SEPARATOR)
        if main_logger:
            main_logger.info(
                f"✅ {filename} | {initial_record_count:,} processed, "
                f"{total_deleted:,} deleted, {total_inserted:,} inserted"
            )

        return {
            "filename": filename,
            "records_before": initial_record_count,
            "records_deleted": total_deleted,
            "records_inserted": total_inserted,
            "final_table_count": final_table_count,
            "processing_time": processing_time,
            "status": "success",
        }

    except Exception as e:
        task_logger.error(LOG_SEPARATOR)
        task_logger.error(f"❌ Error: {e}")
        task_logger.error(LOG_SEPARATOR)
        if main_logger:
            main_logger.error(f"❌ Error processing {filename}: {e}")
        raise

    finally:
        release_lock(task_logger)


async def process_blob(blob_path, client, task_logger, attempt=1):
    try:
        start_time = time.time()
        task_logger.info(
            f"📥 Processing blob: {blob_path} (Attempt {attempt}/{Config.MAX_RETRIES})"
        )
        local_path = PROJECT_ROOT / "data" / "data_incremental" / "incremental" / blob_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob = client.get_blob_client(blob_path)
        try:
            download_stream = await asyncio.wait_for(
                blob.download_blob(), timeout=Config.STREAM_LOAD_TIMEOUT
            )
            content = await download_stream.readall()
        except asyncio.TimeoutError:
            raise TimeoutError(f"Blob download timeout after {Config.STREAM_LOAD_TIMEOUT}s")
        output_path = local_path.with_suffix("")
        if blob_path.lower().endswith(".gz"):
            task_logger.info("🔄 Decompressing gzip file...")
            content = gzip.decompress(content)
        async with aiofiles.open(output_path, "wb") as f:
            await f.write(content)
        file_stem = clean_file_name(output_path.stem)
        parquet_path = (
            PROJECT_ROOT / "data" / "data_incremental" / "raw_parquets" / f"{file_stem}.parquet"
        )
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        task_logger.info(f"🔄 Converting CSV to Parquet: {output_path}")
        df_stream = pl.scan_csv(
            output_path,
            infer_schema_length=0,  # Match simple file's setting
            null_values=["\x00"],  # Only treat \x00 as NULL, matching simple file
            ignore_errors=False,
        )
        # Note: Do NOT apply FactInvoiceSecondary filters here - columns still in CSV format (invoicedate, not invoice_date)
        # Filters will be applied after transformation when columns are renamed to snake_case
        df_stream.sink_parquet(parquet_path, row_group_size=100000)
        output_path.unlink()
        elapsed_time = time.time() - start_time
        task_logger.info(f"✅ Processed {blob_path} in {elapsed_time:.2f}s")
        task_logger.info(f"📊 Parquet file saved to: {parquet_path}")
        return parquet_path
    except Exception as e:
        import traceback

        error_details = f"{type(e).__name__}: {str(e)}"
        stack_trace = traceback.format_exc()

        if attempt < Config.MAX_RETRIES:
            wait = Config.RETRY_DELAY**attempt
            task_logger.error(
                f"❌ Error processing {blob_path}, retrying in {wait:.2f}s (Attempt {attempt}/{Config.MAX_RETRIES})...\n"
                f"Error: {error_details}\n"
                f"Traceback:\n{stack_trace}"
            )
            await asyncio.sleep(wait)
            return await process_blob(blob_path, client, task_logger, attempt + 1)
        else:
            task_logger.error(
                f"❌ Maximum retries reached for {blob_path}.\n"
                f"Error: {error_details}\n"
                f"Traceback:\n{stack_trace}"
            )
            return None


async def transform_dataframe(df, schema_info, stem, task_logger):
    """
    Transform dataframe using centralized transformation engine.

    This function now delegates to the unified transformation engine
    from core.transformers.transformation_engine to ensure consistency
    across all pipelines.

    Args:
        df: Input Polars DataFrame
        schema_info: Schema info (legacy parameter, not used)
        stem: File stem for table name extraction
        task_logger: Logger instance

    Returns:
        Transformed Polars DataFrame
    """
    start_time = time.time()
    try:
        records_before_transform = len(df)

        # Extract table name from file stem (FactInvoiceSecondary_... -> fact_invoice_secondary)
        table_name = get_table_name_from_file(stem)

        task_logger.info(
            f"🔄 Transforming dataframe for {stem}: {records_before_transform:,} records"
        )
        task_logger.info(f"   Table: {table_name}")

        # Use centralized transformation engine
        transformed_df, metadata = validate_and_transform_dataframe(df, table_name, task_logger)

        records_after_transform = len(transformed_df)
        elapsed_time = time.time() - start_time

        # Check for record count mismatch after transformation
        if records_before_transform != records_after_transform:
            task_logger.warning(
                f"⚠️  Transform record mismatch for {stem}: "
                f"Before: {records_before_transform:,} → After: {records_after_transform:,}"
            )

        task_logger.info(
            f"✅ Transform completed for {stem} in {elapsed_time:.2f}s | "
            f"Records: {records_before_transform:,} → {records_after_transform:,}"
        )
        return transformed_df
    except Exception as e:
        task_logger.error(f"❌ Transform error for {stem}: {e}")
        import traceback

        task_logger.error(traceback.format_exc())
        raise


@with_status_tracking(DAILY_FIS_INCREMENTAL_SERVICE_NAME)
async def run_incremental_fis(main_logger=None):
    task_logger = get_pipeline_logger(DAILY_FIS_INCREMENTAL_SERVICE_NAME)
    total_start_time = time.time()
    try:
        task_logger.info(LOG_SEPARATOR)
        task_logger.info("🚀 Starting FIS incremental job")
        task_logger.info(LOG_SEPARATOR)

        current_path = os.getcwd()
        data_dir = "/data/"
        if os.path.exists(data_dir):
            task_logger.info(f"📁 Switching to {data_dir} directory")
            os.chdir(data_dir)
            task_logger.info(f"✅ Changed working directory to {data_dir}")
        else:
            task_logger.info(LOG_SEPARATOR)
            task_logger.warning(
                f"⚠️ {data_dir} directory not found, using current directory: {current_path}"
            )
            task_logger.info(LOG_SEPARATOR)
            if main_logger:
                main_logger.warning(
                    f"⚠️ {data_dir} directory not found, using current directory: {current_path}"
                )

        for path in [
            PROJECT_ROOT / "data" / "data_incremental",
            PROJECT_ROOT / "data" / "data_incremental" / "incremental",
            PROJECT_ROOT / "data" / "data_incremental" / "raw_parquets",
        ]:
            if os.path.exists(path):
                task_logger.info(f"🗑️ Removing existing directory: {path}")
                os.system(f"rm -rf {path}")
            Path(path).mkdir(parents=True, exist_ok=True)
            task_logger.info(f"✅ Created directory: {path}")

        async def load_schema():
            """
            Legacy schema loading function.

            Now that we use the centralized transformation engine,
            schema loading is handled automatically. This function
            returns a dummy schema for backward compatibility.
            """
            task_logger.info("📖 Using centralized transformation engine for schema handling...")
            return {"source": "centralized_engine"}  # Dummy schema, not used anymore

        task_logger.info("📖 Loading schemas...")
        schema = await load_schema()
        if not schema:
            task_logger.error(LOG_SEPARATOR)
            task_logger.error("❌ Schema loading failed, exiting...")
            task_logger.error(LOG_SEPARATOR)
            if main_logger:
                main_logger.error("❌ Schema loading failed, exiting...")
            return
        task_logger.info(LOG_SEPARATOR)

        async with BlobServiceClient(
            account_url=AZURE_CONFIG["account_url"],
            credential=AZURE_CONFIG["sas_token"],
        ) as client:
            container = client.get_container_client(AZURE_CONFIG["container_name"])
            tasks = []
            task_logger.info("🔍 Scanning folders for blobs...")
            for folder in FOLDERS:
                async for blob in container.list_blobs(folder):
                    tasks.append(process_blob(blob.name, container, task_logger))
                    task_logger.info(f"📌 Found blob: {blob.name}")
            task_logger.info(f"⏳ Processing {len(tasks)} blobs in parallel...")
            parquet_files = await asyncio.gather(*tasks)

            task_logger.info("🔄 Transforming parquet files...")
            total_transformed_records = 0
            transformation_results = []
            for pf in filter(None, parquet_files):
                task_logger.info(f"📊 Reading {pf}")
                # Use Polars lazy API for reading Parquet
                df_lazy = pl.scan_parquet(pf)
                df = df_lazy.collect()
                # del df_lazy
                # gc.collect()
                records_before_transform = len(df)

                transformed_df = await transform_dataframe(df, schema, pf.stem, task_logger)
                # del df
                # gc.collect()
                records_after_transform = len(transformed_df)
                total_transformed_records += records_after_transform

                # Store transformation results for summary
                transformation_results.append(
                    {
                        "file": pf.name,
                        "before": records_before_transform,
                        "after": records_after_transform,
                    }
                )

                task_logger.info(
                    f"📊 {pf.name}: {records_before_transform:,} → {records_after_transform:,} records"
                )

                output_path = (
                    PROJECT_ROOT / "data" / "data_incremental" / "cleaned_parquets" / pf.name
                )
                output_path.parent.mkdir(parents=True, exist_ok=True)
                transformed_df.write_parquet(output_path)
                del transformed_df
                gc.collect()
                pf.unlink()
                task_logger.info(f"✅ Saved {pf.name}")

            task_logger.info(
                f"📊 Total records after transformation: {total_transformed_records:,}"
            )

        fis_files = os.listdir(PROJECT_ROOT / "data" / "data_incremental" / "cleaned_parquets")
        if fis_files:
            total_files_processed = 0
            total_records_processed = 0
            total_records_deleted = 0
            total_records_inserted = 0
            processing_results = []

            task_logger.info(f"🔄 Found {len(fis_files)} files to process")

            for file in fis_files:
                task_logger.info(f"🔄 Processing {file} using optimized deletion/insertion...")
                # Get record count before processing
                file_path = PROJECT_ROOT / "data" / "data_incremental" / "cleaned_parquets" / file
                df_temp = pl.read_parquet(file_path)
                file_record_count = df_temp.height
                del df_temp  # Free memory immediately

                result = await process_file(file, task_logger, main_logger)

                total_files_processed += 1
                total_records_processed += file_record_count
                total_records_deleted += result["records_deleted"]
                total_records_inserted += result["records_inserted"]
                processing_results.append(result)

            # Log comprehensive processing summary
            if main_logger:
                main_logger.info(LOG_SEPARATOR)
                main_logger.info("📊 SUMMARY OF PROCESSING")
                main_logger.info(
                    f"Files: {total_files_processed}, "
                    f"Records processed: {total_records_processed:,}, "
                    f"Deleted: {total_records_deleted:,}, "
                    f"Inserted: {total_records_inserted:,}"
                )
                main_logger.info(LOG_SEPARATOR)

            # Check for overall mismatches
            if total_records_processed != total_records_deleted:
                task_logger.warning(LOG_SEPARATOR)
                task_logger.warning(
                    f"⚠️  OVERALL MISMATCH: Expected {total_records_processed:,} deletions "
                    f"but performed {total_records_deleted:,} deletions"
                )
                task_logger.warning(LOG_SEPARATOR)
                if main_logger:
                    main_logger.warning(
                        f"⚠️  OVERALL MISMATCH: Expected {total_records_processed:,} deletions "
                        f"but performed {total_records_deleted:,} deletions"
                    )
            if total_records_processed != total_records_inserted:
                task_logger.warning(LOG_SEPARATOR)
                task_logger.warning(
                    f"⚠️  OVERALL MISMATCH: Expected {total_records_processed:,} insertions "
                    f"but performed {total_records_inserted:,} insertions"
                )
                task_logger.warning(LOG_SEPARATOR)
                if main_logger:
                    main_logger.warning(
                        f"⚠️  OVERALL MISMATCH: Expected {total_records_processed:,} insertions "
                        f"but performed {total_records_inserted:,} insertions"
                    )
        # No pool to close
        total_time = time.time() - total_start_time

        task_logger.info(LOG_SEPARATOR)
        task_logger.info("🎉 Pipeline completed successfully!")
        task_logger.info(f"⏱️  Total time: {total_time:.2f}s")
        task_logger.info(f"📅  Date: {date.today()}")
        task_logger.info(LOG_SEPARATOR)

    except Exception as e:
        task_logger.error(LOG_SEPARATOR)
        task_logger.error(f"❌ Error: {e}")
        task_logger.error(LOG_SEPARATOR)
        if main_logger:
            main_logger.error(f"❌ Error: {e}")
        raise


if __name__ == "__main__":
    task_logger = get_pipeline_logger(DAILY_FIS_INCREMENTAL_SERVICE_NAME)
    task_logger.info(LOG_SEPARATOR)
    task_logger.info("🚀 Starting FIS incremental job...")
    task_logger.info(LOG_SEPARATOR)
    asyncio.run(run_incremental_fis())
