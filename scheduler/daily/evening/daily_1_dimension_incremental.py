"""
Daily Dimension Incremental Load Job for StarRocks (Refactored)

Simplified orchestrator using unified ETL pipeline.
Uses modular components for clean separation of concerns.
"""

import sys
import time
import asyncio
import tracemalloc
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from azure.storage.blob.aio import BlobServiceClient

from utils.pipeline_config import (
    Config,
    DIMENSION_TABLES,
    DAILY_DIMENSION_INCREMENTAL_SERVICE_NAME,
    LOG_SEPARATOR,
)
from utils.logging_utils import get_pipeline_logger, log_summary
from utils.blob_processor_utils import process_blobs_sequentially
from utils.etl_orchestrator import ETLOrchestrator

tracemalloc.start()

# Color codes
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


async def process_dimension_table(
    parquet_path: Path,
    orchestrator: ETLOrchestrator,
    logger,
) -> dict:
    """
    Process a single dimension table using unified ETL pipeline.

    Args:
        parquet_path: Path to parquet file
        orchestrator: ETLOrchestrator instance
        logger: Logger instance

    Returns:
        Result dictionary with processing status and metrics
    """
    try:
        logger.info(f"\n{LOG_SEPARATOR}")
        logger.info(f"Processing: {parquet_path.name}")
        logger.info(f"{LOG_SEPARATOR}")

        # Get table name from filename
        file_stem = parquet_path.stem

        # Extract base table name (remove timestamp suffix like _9999_2025_12_02_14:01:46)
        # Format: DimHierarchy_9999_2025_12_02_14:01:46
        # We want: DimHierarchy
        base_table_name = file_stem.split("_")[0] if "_" in file_stem else file_stem

        # Map filename to table name (all database tables are in snake_case)
        table_mapping = {
            "DimHierarchy": "dim_hierarchy",
            "DimDealerMaster": "dim_dealer_master",
            "DimDealer": "dim_dealer_master",
            "DimCustomerMaster": "dim_customer_master",
            "DimMaterial": "dim_material",
            "DimSalesGroup": "dim_sales_group",
            "FactInvoiceDetails": "fact_invoice_details",
            "FactInvoiceSecondary": "fact_invoice_secondary",
        }

        # Get table name from mapping, fallback to converting CamelCase to snake_case
        if base_table_name in table_mapping:
            table_name = table_mapping[base_table_name]
        else:
            # Convert CamelCase to snake_case as fallback
            import re

            table_name = re.sub(r"(?<!^)(?=[A-Z])", "_", base_table_name).lower()

        logger.info(f"{CYAN}Table: {table_name} (from {file_stem}){RESET}")

        # Run unified ETL pipeline
        success, etl_result = orchestrator.orchestrate(
            parquet_path,
            table_name,
            schema=None,  # Will load from db/column_mappings/
            truncate=True,
        )

        # Extract metrics from result
        rows_loaded = etl_result.get("steps", {}).get("load", {}).get("total_loaded", 0)
        elapsed = etl_result.get("elapsed_seconds", 0)

        if not success:
            logger.error(f"{RED}❌ ETL failed: {etl_result.get('error', 'Unknown error')}{RESET}")
            return {
                "file": parquet_path.name,
                "table": table_name,
                "success": False,
                "rows_loaded": rows_loaded,
                "elapsed_seconds": elapsed,
                "error": etl_result.get("error", "ETL pipeline failed"),
            }

        logger.info(f"{GREEN}✅ Successfully loaded {rows_loaded:,} rows in {elapsed:.2f}s{RESET}")

        return {
            "file": parquet_path.name,
            "table": table_name,
            "success": True,
            "rows_loaded": rows_loaded,
            "elapsed_seconds": elapsed,
        }

    except Exception as e:
        logger.error(f"{RED}❌ Error processing {parquet_path.name}: {e}{RESET}")
        import traceback

        logger.error(traceback.format_exc())
        return {
            "file": parquet_path.name,
            "table": "Unknown",
            "success": False,
            "rows_loaded": 0,
            "elapsed_seconds": 0,
            "error": str(e),
        }


async def run_pipeline(logger) -> dict:
    """
    Main pipeline: Download blobs, convert to parquet, then load to StarRocks.

    Returns:
        Pipeline result dictionary with overall metrics
    """
    total_start_time = time.time()

    try:
        logger.info(f"\n{LOG_SEPARATOR}")
        logger.info(f"🚀 {DAILY_DIMENSION_INCREMENTAL_SERVICE_NAME}")
        logger.info(f"{LOG_SEPARATOR}")

        # PHASE 1: Download and process blobs from Azure
        logger.info(f"\n{CYAN}═══ PHASE 1: BLOB PROCESSING ═══{RESET}")
        logger.info("⬇️  Downloading and converting blobs to parquet...")

        # Connect to Azure Blob Storage
        logger.info("🔗 Connecting to Azure Blob Storage...")

        azure_config = Config.get_azure_config()
        connection_string = f"BlobEndpoint={azure_config['account_url']};SharedAccessSignature={azure_config['sas_token']}"

        async with BlobServiceClient.from_connection_string(connection_string) as client:
            container_client = client.get_container_client(azure_config["container_name"])

            # List all blobs to process
            logger.info("🔍 Scanning Azure blobs...")
            blob_paths = []

            for table_name, folder_path in DIMENSION_TABLES.items():
                logger.info(f"  📁 Scanning {table_name} folder...")
                async for blob in container_client.list_blobs(name_starts_with=folder_path):
                    blob_paths.append(blob.name)

            logger.info(f"{GREEN}✓ Found {len(blob_paths)} blobs{RESET}")

            if not blob_paths:
                logger.warning(f"{YELLOW}⚠️  No blobs found to process{RESET}")
                return {
                    "success": False,
                    "tables_processed": 0,
                    "error": "No blobs found in Azure",
                }

            # Process blobs sequentially (download, decompress, convert to parquet)
            logger.info("📦 Processing blobs (download, decompress, convert to parquet)...")

            output_dir = Config.DATA_INCREMENTAL_PARQUETS_RAW
            output_dir.mkdir(parents=True, exist_ok=True)

            blob_result = await process_blobs_sequentially(
                blob_paths, container_client, output_dir, logger
            )

            parquet_files = blob_result.get("successful", [])
            failed_blobs = blob_result.get("failed", [])

            logger.info(
                f"{GREEN}✓ Blob processing complete: {len(parquet_files)} successful, "
                f"{len(failed_blobs)} failed{RESET}"
            )

            if not parquet_files:
                logger.error(f"{RED}❌ No parquet files generated from blobs{RESET}")
                return {
                    "success": False,
                    "tables_processed": 0,
                    "total_rows": 0,
                    "error": "No parquet files generated",
                }

        # PHASE 2: Load to StarRocks
        logger.info(f"\n{CYAN}═══ PHASE 2: STARROCKS LOADING ═══{RESET}")
        logger.info(f"📊 Loading {len(parquet_files)} tables to StarRocks...")

        # Initialize ETL orchestrator
        orchestrator = ETLOrchestrator()

        # Process each table
        results = []
        total_rows = 0
        successful_tables = 0
        failed_tables = 0

        for parquet_file in parquet_files:
            result = await process_dimension_table(parquet_file, orchestrator, logger)
            results.append(result)

            if result["success"]:
                successful_tables += 1
                total_rows += result.get("rows_loaded", 0)
            else:
                failed_tables += 1

        # PHASE 3: Summary
        total_elapsed = time.time() - total_start_time

        logger.info(f"\n{LOG_SEPARATOR}")
        logger.info("📈 PIPELINE SUMMARY")
        logger.info(f"{LOG_SEPARATOR}")

        logger.info(f"✅ Successful tables: {successful_tables}/{len(parquet_files)}")
        logger.info(f"❌ Failed tables: {failed_tables}/{len(parquet_files)}")
        logger.info(f"📊 Total rows loaded: {total_rows:,}")
        logger.info(f"⏱️  Total elapsed time: {total_elapsed:.2f}s")

        # Print detailed results
        logger.info(f"\n{CYAN}Table-by-table results:{RESET}")
        for result in results:
            status = "✅" if result["success"] else "❌"
            table = result.get("table", "Unknown")
            rows = result.get("rows_loaded", 0)
            elapsed = result.get("elapsed_seconds", 0)
            logger.info(f"  {status} {table:<25} {rows:>10,} rows ({elapsed:>7.2f}s)")

        return {
            "success": failed_tables == 0,
            "tables_processed": len(parquet_files),
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "total_rows": total_rows,
            "elapsed_seconds": total_elapsed,
            "results": results,
        }

    except Exception as e:
        logger.error(f"{RED}Pipeline failed: {e}{RESET}")
        import traceback

        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e),
            "elapsed_seconds": time.time() - total_start_time,
        }


def main():
    """Entry point for the job"""
    logger = get_pipeline_logger(DAILY_DIMENSION_INCREMENTAL_SERVICE_NAME)

    try:
        # Run async pipeline
        result = asyncio.run(run_pipeline(logger))

        # Log summary using the expected format
        success_count = result.get("successful_tables", 0)
        fail_count = result.get("failed_tables", 0)
        total_elapsed = result.get("elapsed_seconds", 0)
        total_rows = result.get("total_rows", 0)

        log_summary(logger, total_elapsed, success_count, fail_count, total_rows)

        # Exit with appropriate code
        exit_code = 0 if result.get("success", False) else 1
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.warning(f"{YELLOW}Pipeline interrupted by user{RESET}")
        sys.exit(130)
    except Exception as e:
        logger.error(f"{RED}Fatal error: {e}{RESET}")
        import traceback

        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Report memory usage
        current, peak = tracemalloc.get_traced_memory()
        logger.info(f"Memory usage: {peak / 1024 / 1024:.1f} MB peak")
        tracemalloc.stop()


if __name__ == "__main__":
    main()
