#!/usr/bin/env python3
"""
Uthra Global - Daily DD Business Logic Processing

Processes dealer-distributor business logic for Uthra Global tenant.
Transforms fact_invoice_details into fact_invoice_secondary format.
"""

import sys
import time
import pymysql
import csv
import tracemalloc
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

tracemalloc.start()

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from orchestration.tenant_manager import TenantManager
from utils.logging_utils import get_pipeline_logger
from core.loaders.starrocks_stream_loader import StarRocksStreamLoader

# Tenant configuration
TENANT_SLUG = "uthra-global"

# Color codes
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"

# Business logic query
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
    AND customer_code != dealer_code
"""


def get_starrocks_connection(tenant_config):
    """Create MySQL connection to StarRocks"""
    return pymysql.connect(
        host=tenant_config.database_host,
        port=tenant_config.database_port,
        user=tenant_config.database_user,
        password=tenant_config.database_password,
        database=tenant_config.database_name,
        charset="utf8mb4",
        autocommit=True,
    )


def main():
    """Main execution function"""

    # Load Uthra Global tenant configuration
    tenant_manager = TenantManager(PROJECT_ROOT / "configs")
    tenant_config = tenant_manager.get_tenant_by_slug(TENANT_SLUG)

    if not tenant_config:
        print(f"{RED}❌ Tenant not found: {TENANT_SLUG}{RESET}")
        sys.exit(1)

    # Set up logging
    logger = get_pipeline_logger(f"uthra_global_dd_logic_{int(time.time())}")

    logger.info("=" * 80)
    logger.info(f"{CYAN}PIDILITE - DAILY DD BUSINESS LOGIC{RESET}")
    logger.info("=" * 80)
    logger.info(f"Tenant: {tenant_config.tenant_name}")
    logger.info(f"Database: {tenant_config.database_name}")
    logger.info("=" * 80)

    overall_start = time.time()

    try:
        # Connect to StarRocks
        logger.info(f"\n{CYAN}[Step 1] Connecting to StarRocks...{RESET}")
        conn = get_starrocks_connection(tenant_config)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # Execute query
        logger.info(f"\n{CYAN}[Step 2] Executing DD logic query...{RESET}")
        cursor.execute(QUERY)

        # Fetch results
        logger.info(f"{CYAN}[Step 3] Fetching results...{RESET}")
        rows = cursor.fetchall()
        total_rows = len(rows)
        logger.info(f"{GREEN}✓ Fetched {total_rows} rows{RESET}")

        if total_rows == 0:
            logger.warning(f"{YELLOW}No rows to process, exiting{RESET}")
            cursor.close()
            conn.close()
            sys.exit(0)

        # Get column names from first row
        column_names = list(rows[0].keys())
        logger.info(f"Columns: {len(column_names)}")

        # Write to CSV
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp_file:
            csv_path = tmp_file.name
            writer = csv.DictWriter(tmp_file, fieldnames=column_names, delimiter='\x01')

            logger.info(f"\n{CYAN}[Step 4] Writing to CSV...{RESET}")
            for row in tqdm(rows, desc="Writing rows", unit="rows"):
                writer.writerow(row)

        logger.info(f"{GREEN}✓ Wrote {total_rows} rows to CSV{RESET}")

        # Load to StarRocks
        logger.info(f"\n{CYAN}[Step 5] Loading to fact_invoice_secondary...{RESET}")

        loader = StarRocksStreamLoader(
            tenant_config=tenant_config,
            logger=logger,
            debug=True,
            max_error_ratio=0.0
        )

        try:
            success, result = loader.stream_load_csv(
                table_name="fact_invoice_secondary",
                csv_file_path=csv_path,
                columns=column_names
            )
        finally:
            if os.path.exists(csv_path):
                os.unlink(csv_path)

        # Check result
        if success and result.get('Status') == 'Success':
            rows_loaded = result.get('NumberLoadedRows', 0)
            logger.info(f"{GREEN}✅ Loaded {rows_loaded} rows successfully{RESET}")
        else:
            logger.error(f"{RED}❌ Load failed: {result.get('Message', 'Unknown error')}{RESET}")
            cursor.close()
            conn.close()
            sys.exit(1)

        # Summary
        overall_time = time.time() - overall_start

        logger.info(f"\n{CYAN}{'=' * 80}{RESET}")
        logger.info(f"{CYAN}PIDILITE DD LOGIC SUMMARY{RESET}")
        logger.info(f"{CYAN}{'=' * 80}{RESET}")
        logger.info(f"Rows processed: {total_rows}")
        logger.info(f"Rows loaded: {rows_loaded}")
        logger.info(f"Total time: {overall_time:.2f}s")
        logger.info(f"{CYAN}{'=' * 80}{RESET}")

        cursor.close()
        conn.close()

        logger.info(f"{GREEN}✅ Job completed successfully{RESET}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"{RED}❌ Fatal error in DD logic job: {str(e)}{RESET}")
        logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
