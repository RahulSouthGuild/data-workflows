from datetime import datetime
import gzip
import asyncio
import time
from pathlib import Path
import polars as pl
from azure.storage.blob.aio import BlobServiceClient
import aiofiles
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import random
import shutil
import sys
import os
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

# Load environment variables from .env.starrocks
load_dotenv(PROJECT_ROOT / ".env.starrocks")

from utils.logging_utils import get_pipeline_logger, with_status_tracking
from utils.pipeline_config import LOG_SEPARATOR, DAILY_BLOB_BACKUP_SERVICE_NAME


def get_base_data_dir():
    """Get base directory for blob backup, respecting configuration."""
    # Use environment variable if set, otherwise use project's data directory
    backup_path = os.getenv("DATA_BACKUP_PATH")

    if backup_path:
        data_dir = Path(backup_path)
    else:
        # Default to project's data/blob-backup directory (user-writable)
        project_root = Path(__file__).parent.parent.parent
        data_dir = project_root / "data" / "blob-backup"

    if not data_dir.exists():
        data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir)


def parse_azure_connection_string(connection_string: str) -> dict:
    """
    Parse Azure connection string to extract account URL and SAS token.
    Format: BlobEndpoint=<url>;SharedAccessSignature=<token>
    """
    parts = {}
    for part in connection_string.split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            parts[key] = value

    account_url = parts.get("BlobEndpoint", "").rstrip("/")
    sas_token = parts.get("SharedAccessSignature", "")

    return {"account_url": account_url, "sas_token": sas_token}


BASE_DATA_DIR = get_base_data_dir()

# Extract Azure config from connection string (from .env.starrocks)
AZURE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_PARSED = parse_azure_connection_string(AZURE_CONNECTION_STRING)
AZURE_CONFIG = {
    "account_url": AZURE_PARSED.get("account_url", ""),
    "container_name": os.getenv("AZURE_CONTAINER_NAME", "synapsedataprod"),
    "sas_token": AZURE_PARSED.get("sas_token", ""),
}

FOLDERS = [
    "Incremental/FactInvoiceDetails/LatestData/",
    "Incremental/FactInvoiceDetails_107_112/LatestData/",
    "Incremental/FactInvoiceSecondary/LatestData/",
    "Incremental/FactInvoiceSecondary_107_112/LatestData/",
]

# S3/DigitalOcean Spaces Configuration (from environment variables)
S3_CONFIG = {
    "bucket_name": os.getenv("S3_BUCKET_NAME", "datawiz"),
    "access_key": os.getenv("S3_ACCESS_KEY", ""),
    "secret_key": os.getenv("S3_SECRET_KEY", ""),
    "endpoint_url": os.getenv("S3_ENDPOINT_URL", "https://datawiz.blr1.digitaloceanspaces.com"),
    "region_name": os.getenv("S3_REGION_NAME", "blr1"),
}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_CONFIG["endpoint_url"],
        aws_access_key_id=S3_CONFIG["access_key"],
        aws_secret_access_key=S3_CONFIG["secret_key"],
        region_name="blr1",
        config=Config(
            retries={"max_attempts": 3},
            connect_timeout=30,
            read_timeout=30,
            max_pool_connections=50,
        ),
    )


async def upload_to_s3_with_retry(
    s3_client,
    file_path: str,
    s3_key: str,
    task_logger,
    main_logger,
    max_retries: int = 5,
) -> bool:
    for attempt in range(max_retries):
        try:
            s3_client.upload_file(str(file_path), S3_CONFIG["bucket_name"], s3_key)
            task_logger.info(f"Successfully uploaded {s3_key} to S3 on attempt {attempt + 1}")
            return True
        except (ClientError, Exception) as e:
            if attempt == max_retries - 1:
                task_logger.error(
                    f"Failed to upload {s3_key} after {max_retries} attempts: {str(e)}"
                )
                if main_logger:
                    main_logger.error(
                        f"Failed to upload {s3_key} after {max_retries} attempts: {str(e)}"
                    )
                print(f"❌ Failed to upload after {max_retries} attempts: {str(e)}")
                return False

            # Exponential backoff with jitter
            wait_time = min(2**attempt + random.uniform(0, 1), 30)
            task_logger.warning(
                f"Upload attempt {attempt + 1}/{max_retries} failed for {s3_key}: {str(e)}"
            )
            if main_logger:
                main_logger.warning(
                    f"Upload attempt {attempt + 1}/{max_retries} failed for {s3_key}: {str(e)}"
                )
            task_logger.info(f"Retrying upload in {wait_time:.2f} seconds...")
            print(f"⚠️ Upload attempt {attempt + 1}/{max_retries} failed: {str(e)}")
            print(f"⏳ Retrying in {wait_time:.2f} seconds...")
            await asyncio.sleep(wait_time)
    return False


async def process_blob(blob_path, client, task_logger, main_logger):
    blob_start_time = time.time()
    try:
        task_logger.info(LOG_SEPARATOR)
        task_logger.info(f"🔄 Starting processing blob: {blob_path}")

        local_path = Path(BASE_DATA_DIR) / blob_path
        local_path.parent.mkdir(parents=True, exist_ok=True)

        parquet_path = local_path.with_suffix(".parquet")
        if parquet_path.exists():
            task_logger.info(f"Removing existing file: {parquet_path}")
            parquet_path.unlink()

        # Download blob
        download_start = time.time()
        blob = client.get_blob_client(blob_path)
        content = await (await blob.download_blob()).readall()
        download_time = time.time() - download_start
        blob_size_mb = len(content) / (1024 * 1024)
        task_logger.info(LOG_SEPARATOR)
        task_logger.info(
            f"📥 Downloaded blob {blob_path} - Size: {blob_size_mb:.2f} MB in {download_time:.2f}s"
        )

        output_path = local_path.with_suffix("")
        if blob_path.lower().endswith(".gz"):
            decompress_start = time.time()
            task_logger.info(LOG_SEPARATOR)
            task_logger.info(f"🗜️ Starting decompression of {blob_path}")
            content = gzip.decompress(content)
            decompress_time = time.time() - decompress_start
            decompressed_size_mb = len(content) / (1024 * 1024)
            task_logger.info(
                f"📦 Decompressed {blob_path} - Size: {decompressed_size_mb:.2f} MB in {decompress_time:.2f}s"
            )
            task_logger.info(LOG_SEPARATOR)

        async with aiofiles.open(output_path, "wb") as f:
            await f.write(content)

        # Convert to Parquet
        conversion_start = time.time()
        task_logger.info(LOG_SEPARATOR)
        task_logger.info(f"📊 Starting CSV to Parquet conversion: {output_path}")

        df_stream = pl.scan_csv(
            output_path,
            infer_schema_length=0,
            null_values=["\x00"],
            ignore_errors=False,
            low_memory=True,  # Use low memory mode
        )

        df_stream.sink_parquet(parquet_path, row_group_size=100000)
        conversion_time = time.time() - conversion_start

        parquet_size = parquet_path.stat().st_size
        parquet_size_mb = parquet_size / (1024 * 1024)
        task_logger.info(LOG_SEPARATOR)
        task_logger.info(
            f"📊 Converted to Parquet - Size: {parquet_size_mb:.2f} MB in {conversion_time:.2f}s"
        )

        output_path.unlink()

        # Upload to S3 with retry
        upload_start = time.time()
        task_logger.info(LOG_SEPARATOR)
        task_logger.info(f"📤 Starting S3 upload for: {parquet_path}")
        s3_client = get_s3_client()
        s3_key = f"{blob_path.replace('.gz', '.parquet')}"
        task_logger.info(f"S3 key: {s3_key}")

        upload_success = await upload_to_s3_with_retry(
            s3_client, parquet_path, s3_key, task_logger, main_logger
        )
        upload_time = time.time() - upload_start

        if not upload_success:
            task_logger.error(f"Failed to upload {s3_key} after all retry attempts")
            if main_logger:
                main_logger.error(f"Failed to upload {s3_key} after all retry attempts")
            print(f"❌ Failed to upload {s3_key} after all retry attempts")
            return None

        task_logger.info(f"Successfully uploaded {s3_key} to S3 in {upload_time:.2f}s")

        # Clean up parquet file after successful upload
        parquet_path.unlink()

        total_time = time.time() - blob_start_time
        task_logger.info(LOG_SEPARATOR)
        task_logger.info(f"✅ Completed processing {blob_path} - Total time: {total_time:.2f}s")
        task_logger.info(LOG_SEPARATOR)
        if main_logger:
            main_logger.info(f"✅ Completed processing {blob_path} in {total_time:.2f}s")
        return parquet_path

    except Exception as e:
        total_time = time.time() - blob_start_time
        task_logger.error(LOG_SEPARATOR)
        task_logger.error(f"❌ Error processing {blob_path} after {total_time:.2f}s: {e}")
        task_logger.error(LOG_SEPARATOR)
        if main_logger:
            main_logger.error(f"❌ Error processing {blob_path} after {total_time:.2f}s: {e}")
        print(f"❌ Error processing {blob_path}: {e}")
        return None


async def process_all_files(task_logger, main_logger):
    process_start_time = time.time()
    try:
        task_logger.info(LOG_SEPARATOR)
        task_logger.info("🚀 Starting blob download and conversion process")
        print("🚀 Starting blob download and conversion process")
        base_dir = Path(BASE_DATA_DIR)

        # Try to clean up existing files but don't fail if directory is busy
        if base_dir.exists():
            cleanup_start = time.time()
            task_logger.info(LOG_SEPARATOR)
            task_logger.info(f"🧹 Attempting to clean up existing directory: {BASE_DATA_DIR}")
            print(f"🧹 Attempting to clean up existing directory: {BASE_DATA_DIR}")
            try:
                # First try to remove individual files
                files_removed = 0
                dirs_removed = 0
                for item in base_dir.glob("**/*"):
                    try:
                        if item.is_file():
                            item.unlink()
                            files_removed += 1
                        elif item.is_dir():
                            shutil.rmtree(item, ignore_errors=True)
                            dirs_removed += 1
                    except Exception as e:
                        task_logger.warning(f"⚠️ Could not remove {item}: {e}")
                        print(f"⚠️ Could not remove {item}: {e}")

                # Then try to remove empty directories
                for item in base_dir.glob("**/*"):
                    try:
                        if item.is_dir():
                            item.rmdir()
                    except Exception:
                        pass

                cleanup_time = time.time() - cleanup_start
                task_logger.info(
                    f"✨ Cleanup completed - Files removed: {files_removed}, Dirs removed: {dirs_removed} in {cleanup_time:.2f}s"
                )
                task_logger.info(LOG_SEPARATOR)
            except Exception as e:
                task_logger.warning(f"⚠️ Could not fully clean directory: {e}")
                print(f"⚠️ Could not fully clean directory: {e}")

        # Ensure base directory exists
        base_dir.mkdir(parents=True, exist_ok=True)
        task_logger.info(f"Using base directory: {BASE_DATA_DIR}")
        print(f"✅ Using base directory: {BASE_DATA_DIR}")

        task_logger.info(LOG_SEPARATOR)
        task_logger.info("🔗 Connecting to Azure Blob Storage...")
        print("🔗 Connecting to Azure Blob Storage...")
        async with BlobServiceClient(
            account_url=AZURE_CONFIG["account_url"],
            credential=AZURE_CONFIG["sas_token"],
        ) as client:
            container = client.get_container_client(AZURE_CONFIG["container_name"])
            tasks = []

            scan_start = time.time()
            task_logger.info(LOG_SEPARATOR)
            task_logger.info("🔍 Scanning folders for blobs...")
            print("🔍 Scanning folders for blobs...")
            blob_count = 0
            for folder in FOLDERS:
                task_logger.info(f"📁 Scanning folder: {folder}")
                async for blob in container.list_blobs(folder):
                    tasks.append(process_blob(blob.name, container, task_logger, main_logger))
                    blob_count += 1
                    task_logger.debug(f"📌 Found blob: {blob.name}")
                    print(f"📌 Found blob: {blob.name}")

            scan_time = time.time() - scan_start
            task_logger.info(f"✅ Scan completed - Found {blob_count} blobs in {scan_time:.2f}s")
            task_logger.info(LOG_SEPARATOR)

            # Process files in batches to avoid memory issues
            batch_size = 4
            total_batches = (len(tasks) + batch_size - 1) // batch_size
            task_logger.info(
                f"Processing {len(tasks)} files in {total_batches} batches of size {batch_size}"
            )

            for i in range(0, len(tasks), batch_size):
                batch_start = time.time()
                batch_num = i // batch_size + 1
                batch = tasks[i : i + batch_size]
                task_logger.info(
                    f"Processing batch {batch_num}/{total_batches} with {len(batch)} files"
                )
                print(f"⏳ Processing batch {batch_num} of {total_batches}")

                await asyncio.gather(*batch)

                batch_time = time.time() - batch_start
                task_logger.info(
                    f"Completed batch {batch_num}/{total_batches} in {batch_time:.2f}s"
                )

        total_time = time.time() - process_start_time
        task_logger.info(
            f"All files processed successfully in {total_time:.2f}s at {datetime.now()}"
        )
        if main_logger:
            main_logger.info(
                f"SUMMARY: All files processed successfully in {total_time:.2f}s at {datetime.now()}"
            )
        print(f"🎉 All files processed successfully! for {datetime.now()}")

    except Exception as e:
        total_time = time.time() - process_start_time
        task_logger.error(f"Main error after {total_time:.2f}s: {e}")
        if main_logger:
            main_logger.error(f"Main error after {total_time:.2f}s: {e}")
        print(f"❌ Main error: {e}")
        raise


async def run_blob_backup(main_logger=None):
    start_time = time.time()
    start_datetime = datetime.now()
    task_logger = get_pipeline_logger(DAILY_BLOB_BACKUP_SERVICE_NAME)
    task_logger.info(LOG_SEPARATOR)
    task_logger.info("🚀 Starting Blob Backup to DO ...")
    task_logger.info(LOG_SEPARATOR)
    try:
        await process_all_files(task_logger, main_logger)

        end_time = time.time()
        end_datetime = datetime.now()
        total_duration = end_time - start_time

        task_logger.info(LOG_SEPARATOR)
        task_logger.info("✅ Blob backup process completed successfully")
        task_logger.info(f"🕒 Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        task_logger.info(f"🕒 End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        task_logger.info(
            f"⏱️ Total execution time: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)"
        )
        task_logger.info(LOG_SEPARATOR)

        if main_logger:
            main_logger.info(LOG_SEPARATOR)
            main_logger.info("✅ Blob backup process completed successfully")
            main_logger.info(
                f"📊 SUMMARY: Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}, End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}, Total execution time: {total_duration:.2f} seconds"
            )
            main_logger.info(LOG_SEPARATOR)

        print(
            f"✅ Blob backup process completed successfully at {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        print(f"⏱️ Total execution time: {total_duration:.2f} seconds")

    except Exception as e:
        end_time = time.time()
        end_datetime = datetime.now()
        total_duration = end_time - start_time

        task_logger.error(LOG_SEPARATOR)
        task_logger.error(f"❌ Blob backup process failed after {total_duration:.2f} seconds: {e}")
        task_logger.error(f"🕒 Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        task_logger.error(f"🕒 End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        task_logger.error(LOG_SEPARATOR)

        if main_logger:
            main_logger.error(LOG_SEPARATOR)
            main_logger.error(
                f"❌ Blob backup process failed after {total_duration:.2f} seconds: {e}"
            )
            main_logger.error(f"🕒 Start time: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            main_logger.error(f"🕒 End time: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            main_logger.error(LOG_SEPARATOR)

        print(f"❌ Blob backup process failed: {e}")
        print(f"⏱️ Failed after: {total_duration:.2f} seconds")
        raise


if __name__ == "__main__":
    task_logger = get_pipeline_logger(DAILY_BLOB_BACKUP_SERVICE_NAME)
    task_logger.info(LOG_SEPARATOR)
    task_logger.info("🚀 Starting daily ONLY Blob Files Backup job ...")
    task_logger.info(LOG_SEPARATOR)
    asyncio.run(run_blob_backup())
