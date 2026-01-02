import sys
import time
import pymysql
import uuid
import requests
import csv
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import tracemalloc

tracemalloc.start()

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent  # Go up 4 levels to reach project root
sys.path.append(str(PROJECT_ROOT))

from utils.pipeline_config import Config, DAILY_DD_LOGIC_SERVICE_NAME, LOG_SEPARATOR
from utils.logging_utils import get_pipeline_logger, with_status_tracking

# Import constants from Config for backward compatibility
FACT_CHUNK_SIZE = Config.FACT_CHUNK_SIZE
FACT_DELETE_CHUNK_SIZE = Config.FACT_DELETE_CHUNK_SIZE

# StarRocks connection configuration
STARROCKS_CONFIG = {
    "host": Config.STARROCKS_HOST,
    "port": Config.STARROCKS_PORT,
    "http_port": Config.STARROCKS_HTTP_PORT,
    "user": Config.STARROCKS_USER,
    "password": Config.STARROCKS_PASSWORD,
    "database": Config.STARROCKS_DATABASE,
}

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


def stream_load_csv(table_name, csv_file_path, chunk_id=None, columns=None, task_logger=None):
    """Load CSV data into StarRocks using Stream Load API"""
    url = f"http://{STARROCKS_CONFIG['host']}:{STARROCKS_CONFIG['http_port']}/api/{STARROCKS_CONFIG['database']}/{table_name}/_stream_load"

    unique_id = str(uuid.uuid4())[:8]
    headers = {
        "label": f"{table_name}_{int(time.time())}_{chunk_id if chunk_id else ''}_{unique_id}",
        "column_separator": "\x01",
        "format": "CSV",
        "max_filter_ratio": "1.0",  # Allow all rows to be filtered (for debugging)
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
        if task_logger:
            task_logger.info(f"Stream Load Response: {result}")

        if result.get("Status") == "Success":
            return True
        else:
            error_msg = result.get("Message", "Unknown error")
            if task_logger:
                task_logger.error(f"Stream Load Error: {error_msg}")
                if "error_log" in result.get("ErrorURL", ""):
                    task_logger.error(f"Error Log URL: {result['ErrorURL']}")
            return False
    except Exception as e:
        if task_logger:
            task_logger.error(f"Stream Load Exception: {str(e)}")
        return False


@with_status_tracking(DAILY_DD_LOGIC_SERVICE_NAME)
def run_dd_logic(main_logger=None):
    task_logger = get_pipeline_logger(DAILY_DD_LOGIC_SERVICE_NAME)
    start_time = time.time()
    start_datetime = datetime.now()

    task_logger.info(LOG_SEPARATOR)
    task_logger.info(
        f"🚀 Starting DD logic processing at {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    task_logger.info(LOG_SEPARATOR)

    try:
        # 1. Connect to StarRocks
        conn = get_starrocks_connection()
        task_logger.info("✅ Connected to StarRocks")

        # 2. Delete existing DD records
        delete_start = time.time()
        try:
            with conn.cursor() as cursor:
                task_logger.info("🗑️  Deleting existing DD records...")
                cursor.execute("DELETE FROM fact_invoice_secondary WHERE record_type = 'DD'")
                deleted_count = cursor.rowcount
            delete_duration = time.time() - delete_start
            task_logger.info(
                f"🗑️  Deleted {deleted_count} existing DD records in {delete_duration:.2f} seconds"
            )
        except Exception as e:
            task_logger.error(f"❌ Error deleting DD records: {e}")
            raise

        # 3. Fetch records from FactInvoiceDetails
        fetch_start = time.time()
        try:
            task_logger.info("📥 Fetching DD records from FactInvoiceDetails...")
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
                f"📥 Fetched {expected_insert_count} DD records in {fetch_duration:.2f} seconds"
            )

            if not records:
                task_logger.warning("⚠️  No DD records found to process")
                if main_logger:
                    main_logger.warning("⚠️  No DD records found to process")
                return
        except Exception as e:
            task_logger.error(f"❌ Error fetching DD records: {e}")
            raise

        # 4. Convert to CSV and use Stream Load for fast bulk insert
        insert_start = time.time()
        try:
            task_logger.info("⬆️  Preparing bulk insert via Stream Load API...")

            # Write records directly to CSV with \x01 separator
            csv_path = Path(f"/tmp/dd_logic_{int(time.time())}.csv")

            if records:
                fieldnames = list(records[0].keys())
                with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.DictWriter(
                        csvfile, fieldnames=fieldnames, delimiter="\x01", lineterminator="\n"
                    )
                    # Don't write header
                    for record in records:
                        # Convert None to 'NULL' string
                        clean_record = {
                            k: "NULL" if v is None else str(v) for k, v in record.items()
                        }
                        writer.writerow(clean_record)

            task_logger.info(f"📝 Created CSV: {csv_path} ({len(records)} records)")
            task_logger.info(
                f"📋 Columns: {', '.join(fieldnames[:5])}... ({len(fieldnames)} total)"
            )

            # Use Stream Load API for fast bulk insert with column mapping
            success = stream_load_csv(
                table_name="fact_invoice_secondary",
                csv_file_path=str(csv_path),
                chunk_id="dd_logic",
                columns=fieldnames,
                task_logger=task_logger,
            )

            if success:
                insert_count = len(records)
                insert_duration = time.time() - insert_start
                task_logger.info(
                    f"⬆️  Inserted {insert_count} records via Stream Load in {insert_duration:.2f} seconds"
                )
            else:
                raise Exception("Stream Load failed")

            # Clean up temp file
            csv_path.unlink()
        except Exception as e:
            task_logger.error(f"❌ Error inserting DD records: {e}")
            raise

        # Calculate total processing time
        total_duration = time.time() - start_time
        end_datetime = datetime.now()

        task_logger.info(LOG_SEPARATOR)
        task_logger.info(
            f"📊 Process Summary:\n"
            f"✅ Successfully processed {expected_insert_count} DD records\n"
            f"⏰ Completed at: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"⚡ Total time: {total_duration:.2f} seconds\n"
            f"📈 Breakdown:\n"
            f"   - Delete: {delete_duration:.2f}s\n"
            f"   - Fetch: {fetch_duration:.2f}s\n"
            f"   - Insert: {insert_duration:.2f}s"
        )
        task_logger.info(LOG_SEPARATOR)
        if main_logger:
            main_logger.info(
                f"✅ DD logic completed: {expected_insert_count} records processed in {total_duration:.2f}s"
            )

    except Exception as e:
        error_msg = f"❌ DD logic processing failed: {str(e)}"
        task_logger.error(LOG_SEPARATOR)
        task_logger.error(error_msg)
        if main_logger:
            main_logger.error(error_msg)
        raise
    finally:
        try:
            conn.close()
            task_logger.info("✅ StarRocks connection closed")
        except Exception as e:
            task_logger.error(f"❌ Error closing connection: {e}")


if __name__ == "__main__":
    task_logger = get_pipeline_logger(DAILY_DD_LOGIC_SERVICE_NAME)
    task_logger.info(LOG_SEPARATOR)
    task_logger.info("🔄 Running DD logic script as main.")
    try:
        run_dd_logic()
        task_logger.info("✅ DD logic script completed successfully.")
        print("[INFO] ✅ DD logic script completed successfully.")
        task_logger.info(LOG_SEPARATOR)
    except Exception as e:
        task_logger.error(LOG_SEPARATOR)
        task_logger.error(f"❌ DD logic script failed: {e}")
        print(f"[ERROR] ❌ DD logic script failed: {e}")
        task_logger.error(LOG_SEPARATOR)
        raise
