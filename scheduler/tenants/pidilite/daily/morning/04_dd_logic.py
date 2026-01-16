#!/usr/bin/env python3
"""
Pidilite - Daily DD Logic

Generates DD (Dealer Direct) records from fact_invoice_details.
Deletes existing DD records from fact_invoice_secondary and inserts
transformed records based on business rules.
"""

import sys
import time
import pymysql
import csv
from pathlib import Path
from datetime import datetime
import tracemalloc

# Add project root to path FIRST, before other imports
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from orchestration.tenant_manager import TenantManager
from utils.logging_utils import get_pipeline_logger
from core.loaders.starrocks_stream_loader import StarRocksStreamLoader

tracemalloc.start()

# Tenant configuration
TENANT_SLUG = "pidilite"

# Color codes
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"

LOG_SEPARATOR = "=" * 80

QUERY = """
SELECT
    revised_net_value_mvg as revised_net_value_mvg,
    active_flag,
    sales_group_code,
    COALESCE(division_code, 0) as division_code,
    customer_code,
    material_code as item_code,
    customer_code as dealer_code,
    COALESCE(invoice_no, '') as invoice_no,
    posting_date as invoice_date,
    CASE
        WHEN mis_type = 'INV' THEN COALESCE(billing_quantity_in_stock_keeping_unit, 0)
        ELSE 0
    END AS volume,
    volume_in_kg,
    volume_in_ltr,
    CASE
        WHEN mis_type = 'CN' THEN net_value * -1
        ELSE COALESCE(net_value, 0)
    END as reporting_value,
    order_no,
    CASE
        WHEN mis_type = 'CN' THEN COALESCE(billing_quantity_in_stock_keeping_unit, 0) * -1
        ELSE 0
    END AS stock_required_quantity,
    customer_code_org as customer_code_original,
    CAST(posting_date / 100 AS INT) as year_month,
    CAST(DATE_FORMAT(NOW(), '%Y%m%d') AS INT) as created_date,
    mis_type,
    reporting_unit,
    CONCAT(customer_code, '_', sales_group_code) as dealer_sg_key,
    net_weight_in_kg,
    CAST(business_area_code AS VARCHAR(10)) as tsi_code,
    rate,
    'DD' as record_type,
    -- Generate composite ID with NULL handling: PostingDate_CustomerCode_InvoiceNo
    CONCAT(
        CAST(posting_date AS VARCHAR),
        '_',
        CAST(customer_code AS VARCHAR),
        '_',
        CASE WHEN invoice_no IS NULL THEN 'NULL' ELSE invoice_no END
    ) as fis_sg_id_cc_in
FROM
    fact_invoice_details
WHERE
    active_flag = '1'
    AND (
        (mis_type IN ('CN', 'INV'))
        OR (
            mis_type = 'FCN'
            AND reason_code IN ('RTDIF', 'SLPMT')
        )
    )
    AND posting_date >= 20230401
"""


def get_starrocks_connection(tenant_config):
    """Create StarRocks MySQL connection using tenant config"""
    return pymysql.connect(
        host=tenant_config.database_host,
        port=tenant_config.database_port,
        user=tenant_config.database_user,
        password=tenant_config.database_password,
        database=tenant_config.database_name,
        charset="utf8mb4",
        autocommit=True,
    )


def run_dd_logic(tenant_config, main_logger=None):
    """
    Run DD logic for a specific tenant.

    Args:
        tenant_config: TenantConfig object with tenant-specific settings
        main_logger: Optional parent logger
    """
    task_logger = get_pipeline_logger(f"{TENANT_SLUG}_dd_logic_{int(time.time())}")
    start_time = time.time()
    start_datetime = datetime.now()

    task_logger.info(LOG_SEPARATOR)
    task_logger.info(
        f"{CYAN}🚀 Starting DD logic processing at {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}{RESET}"
    )
    task_logger.info(f"{CYAN}Tenant: {tenant_config.tenant_name}{RESET}")
    task_logger.info(f"{CYAN}Database: {tenant_config.database_name}{RESET}")
    task_logger.info(LOG_SEPARATOR)

    try:
        # 1. Connect to StarRocks using tenant config
        conn = get_starrocks_connection(tenant_config)
        task_logger.info(f"{GREEN}✅ Connected to StarRocks{RESET}")

        # 2. Delete existing DD records
        delete_start = time.time()
        try:
            with conn.cursor() as cursor:
                task_logger.info(f"{YELLOW}🗑️  Deleting existing DD records...{RESET}")
                cursor.execute("DELETE FROM fact_invoice_secondary WHERE record_type = 'DD'")
                deleted_count = cursor.rowcount
            delete_duration = time.time() - delete_start
            task_logger.info(
                f"{GREEN}🗑️  Deleted {deleted_count} existing DD records in {delete_duration:.2f} seconds{RESET}"
            )
        except Exception as e:
            task_logger.error(f"{RED}❌ Error deleting DD records: {e}{RESET}")
            raise

        # 3. Fetch records from FactInvoiceDetails
        fetch_start = time.time()
        try:
            task_logger.info(f"{CYAN}📥 Fetching DD records from FactInvoiceDetails...{RESET}")
            with conn.cursor() as cursor:
                task_logger.info(f"Executing DD logic query...")
                cursor.execute(QUERY)
                # Fetch all records as list of dicts
                columns = [desc[0] for desc in cursor.description]
                records = []
                for row in cursor.fetchall():
                    records.append(dict(zip(columns, row)))

            expected_insert_count = len(records)
            fetch_duration = time.time() - fetch_start
            task_logger.info(
                f"{GREEN}📥 Fetched {expected_insert_count} DD records in {fetch_duration:.2f} seconds{RESET}"
            )

            if not records:
                task_logger.warning(f"{YELLOW}⚠️  No DD records found to process{RESET}")
                if main_logger:
                    main_logger.warning(f"{YELLOW}⚠️  No DD records found to process{RESET}")
                return
        except Exception as e:
            task_logger.error(f"{RED}❌ Error fetching DD records: {e}{RESET}")
            raise

        # 4. Convert to CSV and use Stream Load for fast bulk insert
        insert_start = time.time()
        try:
            task_logger.info(f"{CYAN}⬆️  Preparing bulk insert via Stream Load API...{RESET}")

            # Get correct column order from database
            with conn.cursor() as cursor:
                cursor.execute("SHOW COLUMNS FROM fact_invoice_secondary")
                db_columns = [row[0] for row in cursor.fetchall()]

            task_logger.info(f"📋 Database has {len(db_columns)} columns")

            # Write records directly to CSV with \x01 separator
            csv_path = Path(f"/tmp/dd_logic_{int(time.time())}.csv")

            if records:
                # Get query columns (order from query result)
                query_columns = list(records[0].keys())

                # Use ALL database columns (not just those in query) to match table structure
                # This ensures all 68 columns are in CSV, with NULL for missing ones
                fieldnames = db_columns

                task_logger.info(f"📋 Query returned {len(query_columns)} columns")
                task_logger.info(f"📋 Writing ALL {len(fieldnames)} database columns to CSV (missing = NULL)")

                # Check for missing columns (in query but not in DB)
                missing_in_db = set(query_columns) - set(db_columns)
                if missing_in_db:
                    task_logger.warning(f"{YELLOW}⚠️  Columns in query but not in database: {missing_in_db}{RESET}")

                # Check for columns in DB but not in query (will be NULL)
                missing_in_query = set(db_columns) - set(query_columns)
                if missing_in_query:
                    task_logger.info(f"ℹ️  Setting {len(missing_in_query)} columns to NULL (not in query)")

                with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(
                        csvfile, fieldnames=fieldnames, delimiter="\x01", lineterminator="\n"
                    )
                    # Don't write header
                    for record in records:
                        # Write ALL columns in database order
                        # Use query values if available, otherwise \N (MySQL NULL marker)
                        clean_record = {
                            k: "\\N" if record.get(k) is None else str(record.get(k, "\\N"))
                            for k in fieldnames
                        }
                        writer.writerow(clean_record)

            task_logger.info(f"📝 Created CSV: {csv_path} ({len(records)} records)")
            task_logger.info(
                f"📋 Column order: {', '.join(fieldnames[:5])}... ({len(fieldnames)} total)"
            )

            # Use centralized Stream Load API with tenant config
            db_config = {
                "host": tenant_config.database_host,
                "port": tenant_config.database_port,
                "http_port": tenant_config.database_http_port,
                "user": tenant_config.database_user,
                "password": tenant_config.database_password,
                "database": tenant_config.database_name,
            }

            with StarRocksStreamLoader(db_config, logger=task_logger) as loader:
                success, result = loader.stream_load_csv(
                    table_name="fact_invoice_secondary",
                    csv_file_path=str(csv_path),
                    chunk_id="dd_logic",
                    columns=fieldnames,
                    null_marker="\\N",  # Use MySQL/StarRocks standard NULL marker
                )

                if not success:
                    error_msg = (
                        result.get("Message", "Unknown error")
                        if isinstance(result, dict)
                        else str(result)
                    )
                    task_logger.error(f"{RED}Stream Load failed: {error_msg}{RESET}")
                    raise Exception(f"Stream Load Error: {error_msg}")

            if success:
                insert_count = len(records)
                insert_duration = time.time() - insert_start
                task_logger.info(
                    f"{GREEN}⬆️  Inserted {insert_count} records via Stream Load in {insert_duration:.2f} seconds{RESET}"
                )
            else:
                raise Exception("Stream Load failed")

            # Clean up temp file
            csv_path.unlink()
        except Exception as e:
            task_logger.error(f"{RED}❌ Error inserting DD records: {e}{RESET}")
            raise

        # Calculate total processing time
        total_duration = time.time() - start_time
        end_datetime = datetime.now()

        task_logger.info(LOG_SEPARATOR)
        task_logger.info(
            f"{GREEN}📊 Process Summary:\n{RESET}"
            f"{GREEN}✅ Successfully processed {expected_insert_count} DD records\n{RESET}"
            f"{CYAN}⏰ Completed at: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}\n{RESET}"
            f"{CYAN}⚡ Total time: {total_duration:.2f} seconds\n{RESET}"
            f"{CYAN}📈 Breakdown:\n{RESET}"
            f"{CYAN}   - Delete: {delete_duration:.2f}s\n{RESET}"
            f"{CYAN}   - Fetch: {fetch_duration:.2f}s\n{RESET}"
            f"{CYAN}   - Insert: {insert_duration:.2f}s{RESET}"
        )
        task_logger.info(LOG_SEPARATOR)
        if main_logger:
            main_logger.info(
                f"{GREEN}✅ DD logic completed: {expected_insert_count} records processed in {total_duration:.2f}s{RESET}"
            )

    except Exception as e:
        error_msg = f"{RED}❌ DD logic processing failed: {str(e)}{RESET}"
        task_logger.error(LOG_SEPARATOR)
        task_logger.error(error_msg)
        if main_logger:
            main_logger.error(error_msg)
        raise
    finally:
        try:
            conn.close()
            task_logger.info(f"{GREEN}✅ StarRocks connection closed{RESET}")
        except Exception as e:
            task_logger.error(f"{RED}❌ Error closing connection: {e}{RESET}")


def main():
    """Main execution function"""
    # Load Pidilite tenant configuration
    tenant_manager = TenantManager(PROJECT_ROOT / "configs")
    tenant_config = tenant_manager.get_tenant_by_slug(TENANT_SLUG)

    if not tenant_config:
        print(f"{RED}❌ Tenant not found: {TENANT_SLUG}{RESET}")
        sys.exit(1)

    task_logger = get_pipeline_logger(f"{TENANT_SLUG}_dd_logic_{int(time.time())}")
    task_logger.info(LOG_SEPARATOR)
    task_logger.info(f"{CYAN}🔄 Running DD logic script as main.{RESET}")

    try:
        run_dd_logic(tenant_config)
        task_logger.info(f"{GREEN}✅ DD logic script completed successfully.{RESET}")
        print(f"{GREEN}[INFO] ✅ DD logic script completed successfully.{RESET}")
        task_logger.info(LOG_SEPARATOR)
        sys.exit(0)
    except Exception as e:
        task_logger.error(LOG_SEPARATOR)
        task_logger.error(f"{RED}❌ DD logic script failed: {e}{RESET}")
        print(f"{RED}[ERROR] ❌ DD logic script failed: {e}{RESET}")
        task_logger.error(LOG_SEPARATOR)
        sys.exit(1)


if __name__ == "__main__":
    main()
