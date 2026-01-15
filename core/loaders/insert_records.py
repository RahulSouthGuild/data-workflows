import time
from pathlib import Path
import polars as pl
import pandas as pd
import tempfile
import os
from tqdm import tqdm
import sys
import pymysql
from colorama import init

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.append(str(PROJECT_ROOT))

from utils.DB_CONFIG import DB_CONFIG  # noqa: E402
from core.loaders.starrocks_stream_loader import StarRocksStreamLoader  # noqa: E402

init(autoreset=True)

# print(DB_CONFIG)  # Commented: unnecessary config output

RED, GREEN, YELLOW, RESET = "\033[31m", "\033[32m", "\033[33m", "\033[0m"
CYAN = "\033[36m"

# StarRocks Stream Load Configuration
STARROCKS_CONFIG = {
    "host": DB_CONFIG["host"],
    "port": DB_CONFIG["port"],
    "http_port": int(os.getenv("STARROCKS_HTTP_PORT", "8040")),
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "database": DB_CONFIG["database"],
}

# Stream Load settings
STREAM_LOAD_TIMEOUT = 1800  # 30 minutes
MAX_ERROR_RATIO = 0.1  # 10% error tolerance
CHUNK_SIZE = 100000  # Records per chunk


def get_starrocks_connection():
    """Create StarRocks MySQL connection for metadata operations"""
    return pymysql.connect(
        host=STARROCKS_CONFIG["host"],
        port=STARROCKS_CONFIG["port"],
        user=STARROCKS_CONFIG["user"],
        password=STARROCKS_CONFIG["password"],
        database=STARROCKS_CONFIG["database"],
        charset="utf8mb4",
        autocommit=True,
    )


# Stream Load is now handled by StarRocksStreamLoader from core.loaders


def get_table_columns(conn, table_name):
    """Get column names from StarRocks table"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DESC {table_name}")
            columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        print(f"{RED}Error getting table columns: {e}{RESET}")
        return []


def delete_existing_records(table_name, stem):
    """Delete existing records based on table type"""
    conn = None
    try:
        conn = get_starrocks_connection()

        if "FactInvoiceSecondary" in stem:
            parts = stem.split("_")
            sales_groups = parts[1:]
            groups_str = ", ".join(f"'{g}'" for g in sales_groups)

            if "DD" in stem:
                print(f"{YELLOW}Deleting fact_invoice_secondary records for RecordType DD{RESET}")
                with conn.cursor() as cursor:
                    cursor.execute("DELETE FROM fact_invoice_secondary WHERE record_type = 'DD'")
            else:
                print(
                    f"{YELLOW}Deleting fact_invoice_secondary records for SalesGroupCodes: {groups_str}{RESET}"
                )
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"DELETE FROM fact_invoice_secondary WHERE sales_group_code IN ({groups_str}) AND record_type != 'DD'"
                    )
            print(f"{GREEN}Deleted records{RESET}")

        elif "FactInvoiceDetails" in stem:
            parts = stem.split("_")
            sales_groups = parts[1:]
            if "107" in sales_groups and "112" in sales_groups:
                additional_groups = [
                    "101",
                    "203",
                    "204",
                    "202",
                    "205",
                    "302",
                    "303",
                    "304",
                    "408",
                    "409",
                    "402",
                    "403",
                    "407",
                    "451",
                    "452",
                    "453",
                    "454",
                    "455",
                    "456",
                    "457",
                    "501",
                    "502",
                    "503",
                    "504",
                    "505",
                    "506",
                    "508",
                    "509",
                    "601",
                    "602",
                    "604",
                    "956",
                    "957",
                    "961",
                    "949",
                    "951",
                    "958",
                    "959",
                    "960",
                ]
                sales_groups.extend([g for g in additional_groups if g not in sales_groups])
            groups_str = ", ".join(f"'{g}'" for g in sales_groups)
            print(
                f"{YELLOW}Deleting fact_invoice_details records for SalesGroupCodes: {groups_str}{RESET}"
            )
            with conn.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM fact_invoice_details WHERE sales_group_code IN ({groups_str})"
                )
            print(f"{GREEN}Deleted records{RESET}")
        else:
            print(f"{YELLOW}Truncating {table_name}...{RESET}")
            with conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            print(f"{GREEN}Truncated {table_name}{RESET}")
    except Exception as e:
        print(f"{RED}Error deleting records: {e}{RESET}")
    finally:
        if conn:
            conn.close()


def process_records(file, delete_existing=True):
    """Process parquet file and load to StarRocks using Stream Load API"""
    start = time.time()
    try:
        print(f"{GREEN}Processing: {file.name}{RESET}")
        stem = file.stem

        # Determine table name (convert to snake_case for database)
        if "FactInvoiceSecondary" in stem:
            table_name = "fact_invoice_secondary"
        elif "FactInvoiceDetails" in stem:
            table_name = "fact_invoice_details"
        elif "DimCustomerMaster" in stem:
            table_name = "dim_customer_master"
        elif "DimDealerMaster" in stem:
            table_name = "dim_dealer_master"
        elif "DimMaterial" in stem:
            table_name = "dim_material"
        elif "DimHierarchy" in stem:
            table_name = "dim_hierarchy"
        elif "DimSalesGroup" in stem:
            table_name = "dim_sales_group"
        elif "DimMaterialMapping" in stem:
            table_name = "dim_material_mapping"
        elif "RlsMaster" in stem:
            table_name = "rls_master"
        else:
            table_name = stem
        print(f"{YELLOW}Table: {table_name}{RESET}")

        # Delete existing records if requested
        if delete_existing:
            delete_existing_records(table_name, stem)

        # Get row count using lazy load (without loading all data)
        lf = pl.scan_parquet(file)
        total = lf.select(pl.len()).collect().item()
        
        # Now load the actual data
        df = pl.read_parquet(file)

        if total == 0:
            print(f"{YELLOW}No records to process{RESET}")
            return

        # CRITICAL: Validate that parquet types match database schema
        print(f"\n{CYAN}Validating data types...{RESET}")
        try:
            conn = get_starrocks_connection()
            cursor = conn.cursor()
            cursor.execute(f"DESC {table_name}")
            db_columns = cursor.fetchall()

            # Build type mapping from database
            db_type_map = {}
            for col_info in db_columns:
                col_name = col_info[0]
                col_type = col_info[1]
                db_type_map[col_name] = col_type

            # Polars type to SQL type mapping
            polars_to_sql = {
                "Int8": "TINYINT",
                "Int16": "SMALLINT",
                "Int32": "INT",
                "Int64": "BIGINT",
                "Float32": "FLOAT",
                "Float64": "DOUBLE",
                "String": "VARCHAR",
            }

            # Check each column in parquet
            type_errors = []
            for col_name in df.columns:
                parquet_type = str(df[col_name].dtype)
                expected_db_type = db_type_map.get(col_name)

                if expected_db_type is None:
                    continue  # Column might be extra, already checked above

                # Get base SQL type for parquet
                parquet_base_type = polars_to_sql.get(parquet_type, parquet_type.upper())
                expected_base_type = expected_db_type.split("(")[0].upper()

                # Check compatibility
                is_compatible = False
                if parquet_base_type == expected_base_type:
                    is_compatible = True
                elif "INT" in parquet_base_type and "INT" in expected_base_type:
                    is_compatible = True
                elif "DOUBLE" in parquet_base_type and (
                    "DOUBLE" in expected_base_type
                    or "FLOAT" in expected_base_type
                    or "DECIMAL" in expected_base_type
                ):
                    # Float64/Double can store decimal values, treat as compatible with DECIMAL
                    is_compatible = True
                elif "FLOAT" in parquet_base_type and (
                    "FLOAT" in expected_base_type or "DECIMAL" in expected_base_type
                ):
                    # Float32 can store decimal values, treat as compatible with DECIMAL
                    is_compatible = True
                elif "VARCHAR" in parquet_base_type and "VARCHAR" in expected_base_type:
                    is_compatible = True

                if not is_compatible:
                    type_errors.append(
                        {
                            "column": col_name,
                            "parquet_type": parquet_type,
                            "db_type": expected_db_type,
                        }
                    )

            if type_errors:
                print(f"\n{RED}❌ CRITICAL: DATA TYPE MISMATCHES - Cannot ingest!{RESET}")
                for error in type_errors:
                    print(
                        f"  {error['column']:<40} Parquet: {error['parquet_type']:<15} Expected: {error['db_type']}"
                    )
                raise ValueError("Data type mismatches detected - see errors above")
            else:
                print(f"{GREEN}✅ All data types match database schema!{RESET}")

            conn.close()
        except Exception as e:
            print(f"{RED}❌ Type validation failed: {e}{RESET}")
            raise

        # Diagnostic: Check CSV schema vs Database schema
        try:
            conn = get_starrocks_connection()
            db_columns = get_table_columns(conn, table_name)
            csv_columns = df.columns
            conn.close()

            # Check for mismatches
            missing_in_db = set(csv_columns) - set(db_columns)
            missing_in_csv = set(db_columns) - set(csv_columns)

            if missing_in_db:
                print(f"{RED}❌ CSV has columns not in {table_name}: {missing_in_db}{RESET}")
            if missing_in_csv:
                print(f"{RED}⚠️  {table_name} has columns not in CSV: {missing_in_csv}{RESET}")

            print(f"{GREEN}✓ CSV columns: {len(csv_columns)}, DB columns: {len(db_columns)}{RESET}")
        except Exception as e:
            print(f"{YELLOW}⚠️  Could not verify schema: {e}{RESET}")

        total_chunks = (total + CHUNK_SIZE - 1) // CHUNK_SIZE
        print(f"{GREEN}Processing {total:,} records in {total_chunks} chunks{RESET}")

        # CRITICAL: Get database columns in correct order for Stream Load mapping
        try:
            conn = get_starrocks_connection()
            with conn.cursor() as cursor:
                cursor.execute(f"DESC {table_name}")
                db_columns_raw = cursor.fetchall()
                db_columns_list = [col[0] for col in db_columns_raw]
            conn.close()

            # Build ordered column list matching database table order
            csv_columns = df.columns
            df_columns_lower = {col.lower(): col for col in csv_columns}

            ordered_columns = []
            ordered_db_columns = []
            missing_columns = []

            for db_col in db_columns_list:
                db_col_lower = db_col.lower()
                if db_col_lower in df_columns_lower:
                    ordered_columns.append(df_columns_lower[db_col_lower])
                    ordered_db_columns.append(db_col)
                else:
                    missing_columns.append(db_col)

            # CRITICAL: Check if any DB columns are missing from parquet
            if missing_columns:
                print(f"{RED}⚠️  WARNING: {len(missing_columns)} columns missing in parquet data!{RESET}")
                print(f"{RED}Missing columns: {missing_columns}{RESET}")
                print(f"{YELLOW}Parquet has: {list(csv_columns)}{RESET}")
                raise ValueError(f"Parquet missing required columns: {missing_columns}")

            # Reorder dataframe to match database table order
            if ordered_columns:
                df = df.select(ordered_columns)

            print(
                f"{GREEN}✓ Column mapping verified - {len(ordered_db_columns)} columns will be loaded{RESET}"
            )
            # print(f"{CYAN}📋 CSV Column Order (Parquet names): {ordered_columns}{RESET}")  # Commented: verbose
            # print(f"{CYAN}📋 DB Column Order (StarRocks names): {ordered_db_columns}{RESET}")  # Commented: verbose
        except Exception as e:
            print(f"{YELLOW}⚠️  Could not verify column order: {e}{RESET}")
            # Fallback: use columns as-is
            ordered_db_columns = list(df.columns)

        successful_chunks = 0
        failed_chunks = 0
        total_rows_loaded = 0
        failed_chunk_details = []

        with tqdm(total=total_chunks, desc="Loading chunks", ncols=100) as pbar:
            for chunk_num, start_row in enumerate(range(0, total, CHUNK_SIZE), 1):
                try:
                    chunk = df.slice(start_row, CHUNK_SIZE)

                    if chunk.is_empty():
                        pbar.update(1)
                        continue

                    # Create temporary CSV file
                    with tempfile.NamedTemporaryFile(
                        mode="w", suffix=".csv", delete=False
                    ) as tmp_file:
                        # Export to CSV with SOH (\x01) delimiter
                        # SOH (Start of Heading) is rarely found in data and avoids issues with commas in fields
                        chunk_pd = chunk.to_pandas()
                        
                        # CRITICAL: Write columns in the exact order specified in ordered_columns
                        # This matches the order sent to StarRocks in the "columns" header
                        if ordered_columns:
                            chunk_pd = chunk_pd[ordered_columns]
                        
                        # Write CSV without converting NaN to empty strings
                        # We'll use the na_rep parameter and null_marker in Stream Load
                        chunk_pd.to_csv(tmp_file.name, sep="\x01", header=False, index=False, na_rep='\\N')

                        # Debug: Show what columns are being sent vs expected
                        # print(f"{CYAN}📤 Sending Stream Load request:{RESET}")  # Commented: verbose
                        # print(f"  CSV columns (order written): {list(chunk_pd.columns)}")  # Commented: verbose
                        # print(f"  DB columns (expected order): {ordered_db_columns}")  # Commented: verbose
                        # print(f"  Total columns: {len(ordered_db_columns)}")  # Commented: verbose
                        # print(f"  Total rows in chunk: {len(chunk_pd)}")  # Commented: verbose
                        # print(f"  CSV file size: {os.path.getsize(tmp_file.name)} bytes")  # Commented: verbose
                        
                        # Sample first few rows to debug data issues
                        # if chunk_num == 1:  # Only show for first chunk to avoid spam  # Commented: debug output
                        #     print(f"\n{CYAN}📊 Sample data (first 3 rows of first 10 columns):{RESET}")
                        #     sample_cols = ordered_db_columns[:10]
                        #     for idx, row_idx in enumerate(range(min(3, len(chunk_pd)))):
                        #         row_data = []
                        #         for col in sample_cols:
                        #             val = chunk_pd.iloc[row_idx][col]
                        #             # Show value, type, and if it's null
                        #             is_null = pd.isna(val)
                        #             row_data.append(f"{col}={repr(val)[:30]}{'(NULL)' if is_null else ''}")
                        #         print(f"  Row {row_idx+1}: {' | '.join(row_data)}")


                        # Load using centralized Stream Loader with explicit column mapping
                        # STRICT: 0% error tolerance - all rows must be valid
                        with StarRocksStreamLoader(STARROCKS_CONFIG, logger=None) as loader:
                            success, result = loader.stream_load_csv(
                                table_name=table_name,
                                csv_file_path=tmp_file.name,
                                chunk_id=chunk_num,
                                columns=ordered_db_columns,
                                null_marker='\\N',  # Use \N for NULL values (MySQL standard)
                            )

                        if success:
                            successful_chunks += 1
                            rows_loaded = result.get("NumberLoadedRows", len(chunk))
                            total_rows_loaded += rows_loaded
                        else:
                            failed_chunks += 1
                            # Extract detailed error info
                            filtered = result.get("NumberFilteredRows", 0)
                            total = result.get("NumberTotalRows", 0)
                            error_msg = result.get("Message", "Unknown error")
                            error_url = result.get("ErrorURL", "No error URL")
                            
                            # Print detailed failure info for debugging
                            print(f"\n{RED}❌ Stream Load FAILED (Chunk {chunk_num}/3){RESET}")
                            print(f"  Status: {result.get('Status', 'Unknown')}")
                            print(f"  Message: {error_msg}")
                            print(f"  Loaded rows: {result.get('NumberLoadedRows', 0)}/{total}")
                            print(f"  Filtered rows: {filtered}/{total} ({100*filtered/total if total > 0 else 0:.1f}%)")
                            print(f"  Error log: {error_url}")
                            
                            failed_chunk_details.append(
                                {
                                    "chunk": chunk_num,
                                    "filtered": filtered,
                                    "total": total,
                                    "message": error_msg,
                                    "error_url": error_url,
                                }
                            )

                        # Clean up temp file
                        os.unlink(tmp_file.name)

                except Exception as e:
                    print(f"{RED}Chunk {chunk_num} error: {e}{RESET}")
                    failed_chunks += 1
                    failed_chunk_details.append({"chunk": chunk_num, "error": str(e)})

                pbar.update(1)

        total_time = time.time() - start
        print(f"\n{GREEN}Loaded {total_rows_loaded:,}/{total:,} rows in {total_time:.2f}s{RESET}")
        
        # Count rows in table after load
        if failed_chunks == 0:  # Only count if all chunks succeeded
            try:
                conn = get_starrocks_connection()
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    db_row_count = cursor.fetchone()[0]
                conn.close()
                
                # Show comparison
                print(f"\n{CYAN}{'='*60}{RESET}")
                print(f"{CYAN}📊 LOAD SUMMARY{RESET}")
                print(f"{CYAN}{'='*60}{RESET}")
                print(f"{GREEN}  Parquet rows (using lazy load): {total:,}{RESET}")
                print(f"{GREEN}  Rows inserted to DB:          {db_row_count:,}{RESET}")
                if total == db_row_count:
                    print(f"{GREEN}  ✅ ALL ROWS LOADED SUCCESSFULLY!{RESET}")
                else:
                    print(f"{RED}  ⚠️  Row count mismatch! Expected {total:,}, got {db_row_count:,}{RESET}")
                print(f"{CYAN}{'='*60}{RESET}\n")
            except Exception as e:
                print(f"{YELLOW}⚠️  Could not verify row count: {e}{RESET}")

        if failed_chunks > 0:
            print(
                f"{RED}❌ Successful chunks: {successful_chunks}, Failed chunks: {failed_chunks}{RESET}"
            )

            # Show summary of failures
            if failed_chunk_details:
                print(f"\n{RED}Failed Chunk Details:{RESET}")
                for detail in failed_chunk_details:
                    print(f"{RED}  Chunk {detail.get('chunk')}:{RESET}")
                    if "error" in detail:
                        print(f"{RED}    Exception: {detail['error']}{RESET}")
                    else:
                        total_rows = detail.get("total", 0)
                        filtered = detail.get("filtered", 0)
                        if total_rows > 0:
                            filter_pct = (filtered / total_rows) * 100
                            print(
                                f"{RED}    Filtered: {filtered:,}/{total_rows:,} ({filter_pct:.1f}%){RESET}"
                            )
                        print(f"{RED}    Message: {detail.get('message', 'Unknown error')}{RESET}")
        else:
            print(
                f"{GREEN}✓ Successful chunks: {successful_chunks}, Failed chunks: {failed_chunks}{RESET}"
            )

    except Exception as e:
        print(f"{RED}Processing error: {str(e)}{RESET}")
        raise


def list_files():
    """List available parquet files"""
    try:
        start_time = time.time()
        cleaned_parquet_path = PROJECT_ROOT / "data" / "data_historical" / "cleaned_parquets"
        parquet_files = list(cleaned_parquet_path.rglob("*.parquet"))
        if not parquet_files:
            print(f"{YELLOW}No parquet files found in {cleaned_parquet_path}.{RESET}")
            return None
        files_dict = {"fact": [], "other": []}
        for f in parquet_files:
            key = "fact" if "FactInvoiceSecondary" in f.stem else "other"
            files_dict[key].append(f)
        files = files_dict["other"] + files_dict["fact"]
        for i, f in enumerate(files, 1):
            try:
                size_bytes = f.stat().st_size
                if size_bytes < 1024:
                    size_str = f"{size_bytes} B"
                elif size_bytes < 1024 * 1024:
                    size_str = f"{size_bytes / 1024:.2f} KB"
                elif size_bytes < 1024 * 1024 * 1024:
                    size_str = f"{size_bytes / (1024 * 1024):.2f} MB"
                else:
                    size_str = f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
                # Optimized: Use scan_parquet to get metadata without loading data
                lf = pl.scan_parquet(f)
                # Get row count from metadata without loading any data
                records = lf.select(pl.len()).collect().item()
                if records < 1000:
                    count_str = f"{records}"
                elif records < 100000:
                    count_str = f"{records / 1000:.1f}K"
                elif records < 10000000:
                    count_str = f"{records / 100000:.1f} lakh"
                else:
                    count_str = f"{records / 10000000:.2f} crore"
                rel_path = f.relative_to(PROJECT_ROOT)
                print(f"{i}. {rel_path} - {size_str}, {count_str} records")
            except Exception as e:
                print(f"{i}. {f.name} - {RED}Error: {str(e)}{RESET}")
        print(f"\n{GREEN}Processed in {time.time() - start_time:.2f}s{RESET}")
        print("\n0. Exit")
        return files
    except Exception as e:
        print(f"{RED}Error listing files: {str(e)}{RESET}")
        return None


def process_file_menu():
    """Interactive menu for file selection and processing"""
    while True:
        print("\n1. Process Specific File")
        print("2. Process All Files")
        print("0. Exit")
        choice = input("Enter choice: ").strip()
        if choice == "0":
            break
        elif choice == "1":
            while True:
                files = list_files()
                if not files:
                    break
                file_choice = input("Enter file number (0 to exit): ").strip()
                if file_choice == "0":
                    break
                try:
                    idx = int(file_choice) - 1
                    if 0 <= idx < len(files):
                        opt = input("Delete existing records? (y/n): ").strip().lower()
                        delete_existing = opt == "y"
                        process_records(files[idx], delete_existing)
                    else:
                        print(f"{RED}Invalid selection{RESET}")
                except Exception as e:
                    print(f"{RED}Invalid input: {str(e)}{RESET}")
                again = input("Process another? (y/n): ").strip().lower()
                if again != "y":
                    break
        elif choice == "2":
            files = list_files()
            if files:
                for file in files:
                    try:
                        print(f"{YELLOW}Processing {file.name}{RESET}")
                        process_records(file)
                    except Exception as e:
                        print(f"{RED}Error processing {file.name}: {str(e)}{RESET}")
        else:
            print(f"{RED}Invalid choice{RESET}")


def main():
    """Main function"""
    try:
        process_file_menu()
    except Exception as e:
        print(f"{RED}Fatal error: {str(e)}{RESET}")


if __name__ == "__main__":
    main()
