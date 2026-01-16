#!/usr/bin/env python3
"""
Pidilite - Daily Fact Invoice Secondary Incremental Load

Loads fact_invoice_secondary table for Pidilite tenant from Azure to StarRocks.
"""

import sys
import time
import asyncio
import tracemalloc
from pathlib import Path

# Add project root to path FIRST, before other imports
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from azure.storage.blob.aio import BlobServiceClient
from orchestration.tenant_manager import TenantManager
from utils.logging_utils import get_pipeline_logger, log_summary
from utils.blob_processor_utils import process_blobs_sequentially
from utils.etl_orchestrator import ETLOrchestrator

tracemalloc.start()

# Tenant configuration
TENANT_SLUG = "pidilite"

# Azure folders for fact_invoice_secondary with folder mapping
# Format: (azure_folder_name,)
FACT_FOLDERS = [
    "FactInvoiceSecondary",
    "FactInvoiceSecondary_107_112",
]

# Color codes
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


async def main():
    """Main execution function"""

    # Load Pidilite tenant configuration
    tenant_manager = TenantManager(PROJECT_ROOT / "configs")
    tenant_config = tenant_manager.get_tenant_by_slug(TENANT_SLUG)

    if not tenant_config:
        print(f"{RED}❌ Tenant not found: {TENANT_SLUG}{RESET}")
        sys.exit(1)

    # Set up logging
    logger = get_pipeline_logger(f"pidilite_fis_incremental_{int(time.time())}")

    logger.info("=" * 80)
    logger.info(f"{CYAN}PIDILITE - DAILY FACT INVOICE SECONDARY INCREMENTAL{RESET}")
    logger.info("=" * 80)
    logger.info(f"Tenant: {tenant_config.tenant_name}")
    logger.info(f"Database: {tenant_config.database_name}")
    logger.info(f"Azure Container: {tenant_config.azure_container_name}")
    logger.info("=" * 80)

    overall_start = time.time()
    results = {"success": [], "failed": []}

    try:
        # Initialize Azure Blob Service Client
        # Support both connection string and account URL + SAS token
        if tenant_config.azure_connection_string:
            blob_service_client = BlobServiceClient.from_connection_string(
                tenant_config.azure_connection_string
            )
        elif tenant_config.azure_account_url and tenant_config.azure_sas_token:
            blob_service_client = BlobServiceClient(
                account_url=tenant_config.azure_account_url,
                credential=tenant_config.azure_sas_token
            )
        else:
            logger.error(f"{RED}❌ Azure credentials not configured. Need either AZURE_STORAGE_CONNECTION_STRING or (AZURE_ACCOUNT_URL + AZURE_SAS_TOKEN){RESET}")
            sys.exit(1)

        async with blob_service_client:
            container_client = blob_service_client.get_container_client(
                tenant_config.azure_container_name
            )

            # Initialize ETL Orchestrator with tenant config
            orchestrator = ETLOrchestrator(
                tenant_config=tenant_config,
                logger=logger
            )

            # Process each fact folder
            for azure_folder in FACT_FOLDERS:
                logger.info(f"\n{CYAN}{'=' * 80}{RESET}")
                logger.info(f"{CYAN}Processing: fact_invoice_secondary from {azure_folder}{RESET}")
                logger.info(f"{CYAN}{'=' * 80}{RESET}")

                # Determine Azure folder path
                folder_path = f"Incremental/{azure_folder}/LatestData/"
                logger.info(f"Azure folder: {folder_path}")

                # List blobs in this folder
                blob_paths = []
                async for blob in container_client.list_blobs(name_starts_with=folder_path):
                    if blob.name.endswith(('.csv', '.gz', '.zip', '.parquet')):
                        blob_paths.append(blob.name)

                if not blob_paths:
                    logger.warning(f"{YELLOW}No blobs found in {folder_path}{RESET}")
                    continue

                logger.info(f"Found {len(blob_paths)} blob(s) to process")

                # Create output directory for raw parquets
                output_dir = tenant_config.data_incremental_path / "raw_parquets"
                output_dir.mkdir(parents=True, exist_ok=True)

                # Download and convert blobs to parquet
                blob_result = await process_blobs_sequentially(
                    blob_paths,
                    container_client,
                    output_dir,
                    logger
                )

                # Process each parquet file with the orchestrator
                for parquet_path in blob_result["successful"]:
                    try:
                        success, result = orchestrator.orchestrate(
                            parquet_path=parquet_path,
                            table_name="fact_invoice_secondary",
                            truncate=False  # Incremental load, don't truncate
                        )
                        if success:
                            results["success"].append(result)
                        else:
                            results["failed"].append(result)
                    except Exception as e:
                        logger.error(f"{RED}Failed to process {parquet_path}: {str(e)}{RESET}")
                        results["failed"].append({
                            "file": str(parquet_path),
                            "error": str(e)
                        })

                # Track failed blob conversions
                for blob_path in blob_result["failed"]:
                    results["failed"].append({
                        "file": blob_path,
                        "error": "Blob download/conversion failed"
                    })

            # Summary
            overall_time = time.time() - overall_start

            logger.info(f"\n{CYAN}{'=' * 80}{RESET}")
            logger.info(f"{CYAN}PIDILITE FACT INVOICE SECONDARY LOAD SUMMARY{RESET}")
            logger.info(f"{CYAN}{'=' * 80}{RESET}")

            log_summary(
                logger=logger,
                total_time=overall_time,
                success_count=len(results["success"]),
                fail_count=len(results["failed"]),
                total_records=sum(r.get("rows_loaded", 0) for r in results["success"])
            )

            logger.info(f"{CYAN}{'=' * 80}{RESET}")

            # Exit with appropriate code
            if results["failed"]:
                logger.error(f"{RED}❌ Job completed with failures{RESET}")
                sys.exit(1)
            else:
                logger.info(f"{GREEN}✅ Job completed successfully{RESET}")
                sys.exit(0)

    except Exception as e:
        logger.error(f"{RED}❌ Fatal error in FIS load job: {str(e)}{RESET}")
        logger.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
