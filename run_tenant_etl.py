#!/usr/bin/env python3
"""
Run ETL Pipeline for a Specific Tenant

This script extracts data from Azure, transforms it, and loads to StarRocks
for a specific tenant.

Usage:
    python run_tenant_etl.py --tenant-slug pidilite --table fact_invoice_secondary
    python run_tenant_etl.py --tenant-slug pidilite --table dim_customer_master
"""

import argparse
import asyncio
import sys
from pathlib import Path
from datetime import datetime
import logging

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.append(str(PROJECT_ROOT))

from orchestration.tenant_manager import TenantManager
from core.extractors.azure_extractor import AzureExtractor
from core.transformers.transformation_engine import validate_and_transform_dataframe, generate_computed_columns
from core.loaders.starrocks_stream_loader import StarRocksStreamLoader
import polars as pl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Table to folder mapping for Azure blob storage
TABLE_FOLDER_MAP = {
    'fact_invoice_secondary': [
        'Incremental/FactInvoiceSecondary/LatestData/',
        'Incremental/FactInvoiceSecondary_107_112/LatestData/',
    ],
    'fact_invoice_details': [
        'Incremental/FactInvoiceDetails/LatestData/',
    ],
    'dim_customer_master': [
        'Incremental/DimCustomerMaster/LatestData/',
    ],
    'dim_dealer_master': [
        'Incremental/DimDealerMaster/LatestData/',
    ],
    'dim_material': [
        'Incremental/DimMaterial/LatestData/',
    ],
    'dim_hierarchy': [
        'Incremental/DimHierarchy/LatestData/',
    ],
}


async def run_etl_for_tenant(tenant_slug: str, table_name: str):
    """
    Run complete ETL pipeline for a tenant and table

    Args:
        tenant_slug: Tenant slug (e.g., 'pidilite')
        table_name: Table to process (e.g., 'fact_invoice_secondary')
    """
    logger.info("="*80)
    logger.info(f"ETL PIPELINE - {tenant_slug.upper()} - {table_name}")
    logger.info("="*80)

    # Step 1: Load tenant configuration
    logger.info("\n[Step 1] Loading tenant configuration...")
    tenant_manager = TenantManager(Path("configs"))
    tenant_config = tenant_manager.get_tenant_by_slug(tenant_slug)

    if not tenant_config:
        logger.error(f"❌ Tenant not found: {tenant_slug}")
        logger.info("\nAvailable tenants:")
        for tenant in tenant_manager.get_all_enabled_tenants():
            logger.info(f"  - {tenant.tenant_name} (slug: {tenant.tenant_slug})")
        return False

    logger.info(f"✓ Tenant: {tenant_config.tenant_name}")
    logger.info(f"  - Database: {tenant_config.database_name}")
    logger.info(f"  - Azure Container: {tenant_config.azure_container}")

    # Get folders for this table
    folders = TABLE_FOLDER_MAP.get(table_name, [])
    if not folders:
        logger.error(f"❌ No folder mapping found for table: {table_name}")
        logger.info(f"\nAvailable tables: {list(TABLE_FOLDER_MAP.keys())}")
        return False

    logger.info(f"  - Azure Folders: {len(folders)}")

    # Step 2: Extract from Azure
    logger.info(f"\n[Step 2] Extracting data from Azure...")
    logger.info(f"  - Container: {tenant_config.azure_container}")

    extractor = AzureExtractor(
        connection_string=tenant_config.azure_connection_string,
        container_name=tenant_config.azure_container,
        logger=logger
    )

    all_dataframes = []

    for folder in folders:
        logger.info(f"  - Processing folder: {folder}")

        # List blobs in folder
        blobs = await extractor.list_blobs_in_folder(folder)
        logger.info(f"    Found {len(blobs)} files")

        for blob_name in blobs[:5]:  # Limit to first 5 files for testing
            logger.info(f"    - Downloading: {blob_name}")

            # Download and read
            blob_data = await extractor.download_blob(blob_name)

            # If it's a zip, extract CSV
            if blob_name.endswith('.zip'):
                csv_data = await extractor.extract_csv_from_zip(blob_data)
                df = pl.read_csv(csv_data)
            else:
                df = pl.read_csv(blob_data)

            all_dataframes.append(df)
            logger.info(f"      Extracted {len(df)} rows")

    if not all_dataframes:
        logger.error("❌ No data extracted from Azure")
        return False

    # Combine all dataframes
    combined_df = pl.concat(all_dataframes)
    logger.info(f"✓ Extracted total: {len(combined_df)} rows from {len(all_dataframes)} files")

    # Step 3: Transform
    logger.info(f"\n[Step 3] Transforming data...")

    # Validate and transform (uses shared schemas for now)
    transformed_df, metadata = validate_and_transform_dataframe(
        df=combined_df,
        table_name=table_name,
        tenant_config=None,  # Use shared schemas (tenant YAML schemas not supported yet)
        logger=logger
    )

    logger.info(f"✓ Transformation successful")
    logger.info(f"  - Rows: {metadata.get('total_rows', 0)}")
    logger.info(f"  - Columns: {len(transformed_df.columns)}")

    # Generate computed columns (uses tenant-specific config)
    final_df = generate_computed_columns(
        df=transformed_df,
        table_name=table_name,
        tenant_config=tenant_config,
        logger=logger
    )

    logger.info(f"✓ Computed columns generated")
    logger.info(f"  - Final columns: {len(final_df.columns)}")

    # Step 4: Load to StarRocks
    logger.info(f"\n[Step 4] Loading to StarRocks...")
    logger.info(f"  - Database: {tenant_config.database_name}")
    logger.info(f"  - Table: {table_name}")

    loader = StarRocksStreamLoader(
        tenant_config=tenant_config,
        logger=logger,
        debug=True,
        max_error_ratio=0.0
    )

    # Write to temporary CSV
    import tempfile
    import os

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
        csv_path = tmp_file.name

    final_df.write_csv(csv_path, separator="\x01", include_header=False)

    try:
        success, result = loader.stream_load_csv(
            table_name=table_name,
            csv_file_path=csv_path,
            columns=final_df.columns
        )
    finally:
        if os.path.exists(csv_path):
            os.unlink(csv_path)

    if success and result.get('Status') == 'Success':
        logger.info(f"✅ Load successful!")
        logger.info(f"  - Rows loaded: {result.get('NumberLoadedRows', 0)}")
        logger.info(f"  - Load time: {result.get('LoadTimeMs', 0)} ms")
    else:
        logger.error(f"❌ Load failed!")
        logger.error(f"  - Status: {result.get('Status')}")
        logger.error(f"  - Message: {result.get('Message', 'N/A')}")
        return False

    # Summary
    logger.info("\n" + "="*80)
    logger.info("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("="*80)
    logger.info(f"Tenant: {tenant_config.tenant_name}")
    logger.info(f"Table: {table_name}")
    logger.info(f"Rows processed: {len(final_df)}")
    logger.info("="*80)

    return True


def main():
    parser = argparse.ArgumentParser(description='Run ETL pipeline for a specific tenant')
    parser.add_argument(
        '--tenant-slug',
        required=True,
        help='Tenant slug (e.g., pidilite)'
    )
    parser.add_argument(
        '--table',
        required=True,
        help='Table name to process'
    )

    args = parser.parse_args()

    # Run async ETL
    success = asyncio.run(run_etl_for_tenant(args.tenant_slug, args.table))

    if not success:
        logger.error("\n❌ ETL pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
