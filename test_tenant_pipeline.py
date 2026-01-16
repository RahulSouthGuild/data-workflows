#!/usr/bin/env python3
"""
Test ETL Pipeline for a Specific Tenant

Usage:
    python test_tenant_pipeline.py --tenant-id <tenant_id>
    python test_tenant_pipeline.py --tenant-slug pidilite
"""

import argparse
from pathlib import Path
import polars as pl
from datetime import datetime
import logging

from orchestration.tenant_manager import TenantManager
from core.transformers.transformation_engine import validate_and_transform_dataframe, generate_computed_columns
from core.loaders.starrocks_stream_loader import StarRocksStreamLoader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_sample_data(table_name: str) -> pl.DataFrame:
    """Create sample data for testing"""

    if table_name == "fact_invoice_secondary":
        # Sample secondary sales data matching the schema
        return pl.DataFrame({
            "invoice_number": ["INV001", "INV002", "INV003"],
            "invoice_date": [20240115, 20240116, 20240117],  # Int32 format: YYYYMMDD
            "customer_code": ["C001", "C002", "C003"],
            "material_code": ["M001", "M002", "M003"],
            "quantity": [100, 200, 150],
            "amount": [10000.50, 25000.75, 15000.00],
            "dealer_code": ["D001", "D002", "D003"],
            "region": ["North", "South", "East"],
        })

    elif table_name == "dim_customer_master":
        # Sample customer master data
        return pl.DataFrame({
            "customer_code": ["C001", "C002", "C003"],
            "customer_name": ["ABC Corp", "XYZ Ltd", "PQR Industries"],
            "city": ["Mumbai", "Delhi", "Bangalore"],
            "state": ["Maharashtra", "Delhi", "Karnataka"],
            "region": ["West", "North", "South"],
        })

    else:
        logger.warning(f"No sample data defined for table: {table_name}")
        return pl.DataFrame()


def test_pipeline_for_tenant(tenant_id_or_slug: str, table_name: str = "fact_invoice_secondary"):
    """
    Test the complete ETL pipeline for a specific tenant

    Args:
        tenant_id_or_slug: Either tenant UUID or tenant slug (e.g., 'pidilite')
        table_name: Name of the table to test with
    """
    logger.info("="*80)
    logger.info("TENANT-AWARE ETL PIPELINE TEST")
    logger.info("="*80)

    # Step 1: Load tenant configuration
    logger.info("\n[Step 1] Loading tenant configuration...")
    tenant_manager = TenantManager(Path("configs"))

    # Try to get tenant by slug or ID
    if "-" in tenant_id_or_slug and len(tenant_id_or_slug) == 36:
        # Looks like a UUID
        tenant_config = tenant_manager.get_tenant(tenant_id_or_slug)
    else:
        # Treat as slug
        tenant_config = tenant_manager.get_tenant_by_slug(tenant_id_or_slug)

    if not tenant_config:
        logger.error(f"❌ Tenant not found: {tenant_id_or_slug}")
        logger.info("\nAvailable tenants:")
        for tenant in tenant_manager.get_all_enabled_tenants():
            logger.info(f"  - {tenant.tenant_name} (slug: {tenant.tenant_slug}, id: {tenant.tenant_id})")
        return False

    logger.info(f"✓ Loaded tenant: {tenant_config.tenant_name}")
    logger.info(f"  - Tenant ID: {tenant_config.tenant_id}")
    logger.info(f"  - Tenant Slug: {tenant_config.tenant_slug}")
    logger.info(f"  - Database: {tenant_config.database_name}")
    logger.info(f"  - User: {tenant_config.database_user}")
    logger.info(f"  - Host: {tenant_config.database_host}:{tenant_config.database_port}")

    # Step 2: Extract (simulate with sample data)
    logger.info(f"\n[Step 2] Extracting sample data for table: {table_name}...")
    raw_df = create_sample_data(table_name)

    if raw_df.is_empty():
        logger.error(f"❌ No sample data available for table: {table_name}")
        return False

    logger.info(f"✓ Extracted {len(raw_df)} rows")
    logger.info(f"  Columns: {raw_df.columns}")

    # Step 3: Transform with tenant-specific schemas
    logger.info(f"\n[Step 3] Transforming data using tenant-specific schemas...")
    logger.info(f"  - Schema path: {tenant_config.schema_path}")
    logger.info(f"  - Column mappings: {tenant_config.column_mappings_path}")
    logger.info(f"  - Computed columns: {tenant_config.computed_columns_path}")

    try:
        # NOTE: For now, transformation engine uses shared schemas from db/schemas
        # because SchemaValidator only supports .py files, not .yaml files yet
        # The tenant-specific .yaml schemas will be used once SchemaValidator is updated

        # Validate and transform
        transformed_df, metadata = validate_and_transform_dataframe(
            df=raw_df,
            table_name=table_name,
            tenant_config=None,  # Use shared schemas for now
            logger=logger
        )

        logger.info(f"✓ Transformation successful")
        logger.info(f"  - Rows: {metadata.get('total_rows', 0)}")
        logger.info(f"  - Columns after transform: {len(transformed_df.columns)}")

        # Generate computed columns (uses tenant-specific computed_columns.yaml)
        final_df = generate_computed_columns(
            df=transformed_df,
            table_name=table_name,
            tenant_config=tenant_config,  # This works - uses tenant's computed_columns.yaml
            logger=logger
        )

        logger.info(f"✓ Computed columns generated")
        logger.info(f"  - Final columns: {len(final_df.columns)}")

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}", exc_info=True)
        return False

    # Step 4: Load to tenant's database
    logger.info(f"\n[Step 4] Loading data to {tenant_config.database_name}...")

    try:
        loader = StarRocksStreamLoader(
            tenant_config=tenant_config,
            logger=logger,
            debug=True,
            max_error_ratio=0.0
        )

        logger.info(f"✓ StarRocks loader initialized")
        logger.info(f"  - Target database: {tenant_config.database_name}")
        logger.info(f"  - Target table: {table_name}")
        logger.info(f"  - HTTP endpoint: {tenant_config.database_host}:{tenant_config.database_http_port}")

        # Convert DataFrame to CSV (same as production pipeline)
        import tempfile
        import os

        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            csv_path = tmp_file.name

        # Write CSV with SOH separator (\x01) and no header (StarRocks standard)
        final_df.write_csv(csv_path, separator="\x01", include_header=False)
        logger.info(f"  - Converted {len(final_df)} rows to CSV")

        # Get column order from DataFrame
        columns = final_df.columns
        logger.info(f"  - Column mapping: {columns}")

        try:
            # Load to StarRocks via Stream Load API with column specification
            success, result = loader.stream_load_csv(
                table_name=table_name,
                csv_file_path=csv_path,
                columns=columns  # Tell StarRocks which columns we're providing
            )
        finally:
            # Clean up temp file
            if os.path.exists(csv_path):
                os.unlink(csv_path)

        if success and result.get('Status') == 'Success':
            logger.info(f"✅ Stream load successful!")
            logger.info(f"  - Rows loaded: {result.get('NumberLoadedRows', 0)}")
            logger.info(f"  - Load time: {result.get('LoadTimeMs', 0)} ms")
            logger.info(f"  - Transaction ID: {result.get('TxnId', 'N/A')}")
        else:
            logger.error(f"❌ Stream load failed!")
            logger.error(f"  - Status: {result.get('Status')}")
            logger.error(f"  - Message: {result.get('Message', 'N/A')}")
            return False

    except Exception as e:
        logger.error(f"❌ Load failed: {e}", exc_info=True)
        return False

    # Step 5: Summary
    logger.info("\n" + "="*80)
    logger.info("✅ PIPELINE TEST COMPLETED SUCCESSFULLY")
    logger.info("="*80)
    logger.info(f"Tenant: {tenant_config.tenant_name} ({tenant_config.tenant_slug})")
    logger.info(f"Table: {table_name}")
    logger.info(f"Database: {tenant_config.database_name}")
    logger.info(f"Rows processed: {len(final_df)}")
    logger.info("="*80)

    return True


def main():
    parser = argparse.ArgumentParser(description='Test ETL pipeline for a specific tenant')
    parser.add_argument(
        '--tenant-id',
        help='Tenant UUID (e.g., 3607d64c-c13f-40bb-ba76-1339b1590bf5)'
    )
    parser.add_argument(
        '--tenant-slug',
        help='Tenant slug (e.g., pidilite)'
    )
    parser.add_argument(
        '--table',
        default='fact_invoice_secondary',
        help='Table name to test with (default: fact_invoice_secondary)'
    )
    parser.add_argument(
        '--list-tenants',
        action='store_true',
        help='List all available tenants'
    )

    args = parser.parse_args()

    # List tenants if requested
    if args.list_tenants:
        logger.info("Available tenants:")
        tenant_manager = TenantManager(Path("configs"))
        for tenant in tenant_manager.get_all_enabled_tenants():
            logger.info(f"\n  Tenant: {tenant.tenant_name}")
            logger.info(f"    - ID: {tenant.tenant_id}")
            logger.info(f"    - Slug: {tenant.tenant_slug}")
            logger.info(f"    - Database: {tenant.database_name}")
            logger.info(f"    - Enabled: {tenant.enabled}")
            logger.info(f"    - Priority: {tenant.schedule_priority}")
        return

    # Determine tenant identifier
    tenant_identifier = args.tenant_slug or args.tenant_id

    if not tenant_identifier:
        logger.error("❌ Please provide either --tenant-id or --tenant-slug")
        logger.info("\nExample usage:")
        logger.info("  python test_tenant_pipeline.py --tenant-slug pidilite")
        logger.info("  python test_tenant_pipeline.py --tenant-id 3607d64c-c13f-40bb-ba76-1339b1590bf5")
        logger.info("  python test_tenant_pipeline.py --list-tenants")
        return

    # Run the test
    success = test_pipeline_for_tenant(tenant_identifier, args.table)

    if not success:
        logger.error("\n❌ Pipeline test failed. Check the logs above for details.")
        exit(1)


if __name__ == "__main__":
    main()
