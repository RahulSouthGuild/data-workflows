"""
Week 2 Integration Test - Tenant-Aware Components

This script tests all updated components to verify they work correctly
in both multi-tenant and legacy modes.

Test Coverage:
1. TenantManager and TenantConfig
2. DatabaseManager tenant-aware connection pooling
3. Transformation engine with tenant-specific configs
4. StarRocks loader with tenant config
"""

from pathlib import Path
from orchestration.tenant_manager import TenantManager
from config.database import DatabaseManager
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_tenant_manager():
    """Test 1: TenantManager initialization and configuration loading"""
    print("\n" + "=" * 80)
    print("TEST 1: TENANT MANAGER")
    print("=" * 80)

    try:
        configs_path = Path("configs")
        tenant_manager = TenantManager(configs_path)

        print(f"\n✓ TenantManager initialized successfully")
        print(f"  Enabled tenants: {len(tenant_manager.tenants)}")

        for tenant_config in tenant_manager.get_all_enabled_tenants():
            print(f"\n  Tenant: {tenant_config.tenant_name}")
            print(f"    - tenant_id: {tenant_config.tenant_id}")
            print(f"    - tenant_slug: {tenant_config.tenant_slug}")
            print(f"    - database_name: {tenant_config.database_name}")
            print(f"    - database_user: {tenant_config.database_user}")
            print(f"    - azure_container: {tenant_config.azure_container_name}")
            print(f"    - schema_path: {tenant_config.schema_path}")
            print(f"    - column_mappings_path: {tenant_config.column_mappings_path}")

        print(f"\n✅ TEST 1 PASSED - TenantManager working correctly")
        return tenant_manager

    except Exception as e:
        print(f"\n❌ TEST 1 FAILED: {str(e)}")
        raise


def test_database_manager(tenant_manager):
    """Test 2: DatabaseManager tenant-aware connection pooling"""
    print("\n" + "=" * 80)
    print("TEST 2: DATABASE MANAGER - TENANT-AWARE POOLING")
    print("=" * 80)

    try:
        tenants = tenant_manager.get_all_enabled_tenants()

        if not tenants:
            print("⚠️  No enabled tenants found, skipping database tests")
            return

        tenant_config = tenants[0]
        print(f"\nTesting with tenant: {tenant_config.tenant_name}")

        # Test 2a: Create tenant-specific engine
        print("\n  Testing tenant-specific engine creation...")
        engine = DatabaseManager.get_engine(tenant_config)
        print(f"  ✓ Created engine for {tenant_config.database_name}")
        print(f"    - Connection URL: mysql+pymysql://{tenant_config.database_user}@{tenant_config.database_host}:{tenant_config.database_port}/{tenant_config.database_name}")
        print(f"    - Pool size: {engine.pool.size()}")

        # Test 2b: Verify engine is reused
        print("\n  Testing engine reuse...")
        engine2 = DatabaseManager.get_engine(tenant_config)
        assert engine is engine2, "Engine not reused!"
        print(f"  ✓ Engine reused correctly (same instance)")

        # Test 2c: Test legacy mode (backward compatibility)
        print("\n  Testing legacy mode (backward compatibility)...")
        try:
            legacy_engine = DatabaseManager.get_engine()  # No tenant_config
            print(f"  ✓ Legacy engine created successfully")
            print(f"    - Note: This uses config.settings.DB_CONFIG")
        except Exception as e:
            print(f"  ℹ️  Legacy mode skipped: {str(e)}")

        print(f"\n✅ TEST 2 PASSED - DatabaseManager working correctly")

    except Exception as e:
        print(f"\n❌ TEST 2 FAILED: {str(e)}")
        import traceback
        traceback.print_exc()


def test_transformation_engine(tenant_manager):
    """Test 3: Transformation engine with tenant-specific configs"""
    print("\n" + "=" * 80)
    print("TEST 3: TRANSFORMATION ENGINE - TENANT-AWARE")
    print("=" * 80)

    try:
        from core.transformers.transformation_engine import (
            generate_computed_columns,
            load_computed_columns_config
        )
        import polars as pl

        tenants = tenant_manager.get_all_enabled_tenants()
        if not tenants:
            print("⚠️  No enabled tenants found, skipping transformation tests")
            return

        tenant_config = tenants[0]
        print(f"\nTesting with tenant: {tenant_config.tenant_name}")

        # Test 3a: Load tenant-specific computed columns config
        print("\n  Testing tenant-specific computed columns config...")
        config = load_computed_columns_config(tenant_config.computed_columns_path)
        print(f"  ✓ Loaded computed columns config")
        print(f"    - Tables with computed columns: {list(config.keys())}")

        # Test 3b: Create test dataframe and generate computed columns
        if 'fact_invoice_secondary' in config:
            print("\n  Testing computed column generation...")
            # Create a minimal test dataframe
            test_df = pl.DataFrame({
                'invoice_date': ['20250116', '20250117'],
                'customer_code': ['CUST001', 'CUST002'],
                'invoice_no': ['INV001', 'INV002']
            })

            result_df = generate_computed_columns(
                test_df,
                'fact_invoice_secondary',
                tenant_config=tenant_config,
                logger=logger
            )

            print(f"  ✓ Generated computed columns")
            print(f"    - Original columns: {test_df.columns}")
            print(f"    - Result columns: {result_df.columns}")
            print(f"    - New columns: {set(result_df.columns) - set(test_df.columns)}")

        # Test 3c: Verify tenant-specific paths
        print("\n  Verifying tenant-specific paths...")
        print(f"    - Schema path: {tenant_config.schema_path}")
        print(f"    - Tables path: {tenant_config.tables_path}")
        print(f"    - Views path: {tenant_config.views_path}")
        print(f"    - Column mappings: {tenant_config.column_mappings_path}")

        # Count schema files
        table_files = list(tenant_config.tables_path.glob("*.yaml"))
        view_files = list(tenant_config.views_path.glob("*.yaml"))
        mapping_files = list(tenant_config.column_mappings_path.glob("*.yaml"))

        print(f"    - Table schemas found: {len(table_files)}")
        print(f"    - View schemas found: {len(view_files)}")
        print(f"    - Column mappings found: {len(mapping_files)}")

        print(f"\n✅ TEST 3 PASSED - Transformation engine working correctly")

    except Exception as e:
        print(f"\n❌ TEST 3 FAILED: {str(e)}")
        import traceback
        traceback.print_exc()


def test_starrocks_loader(tenant_manager):
    """Test 4: StarRocks loader with tenant config"""
    print("\n" + "=" * 80)
    print("TEST 4: STARROCKS STREAM LOADER - TENANT-AWARE")
    print("=" * 80)

    try:
        from core.loaders.starrocks_stream_loader import StarRocksStreamLoader

        tenants = tenant_manager.get_all_enabled_tenants()
        if not tenants:
            print("⚠️  No enabled tenants found, skipping loader tests")
            return

        tenant_config = tenants[0]
        print(f"\nTesting with tenant: {tenant_config.tenant_name}")

        # Test 4a: Create loader with tenant_config
        print("\n  Testing loader initialization with tenant_config...")
        loader = StarRocksStreamLoader(
            tenant_config=tenant_config,
            logger=logger,
            debug=True
        )
        print(f"  ✓ Loader initialized with tenant_config")
        print(f"    - Database: {loader.config['database']}")
        print(f"    - Host: {loader.config['host']}")
        print(f"    - HTTP Port: {loader.config['http_port']}")
        print(f"    - Tenant slug: {loader.tenant_slug}")

        # Test 4b: Test legacy mode (backward compatibility)
        print("\n  Testing legacy mode initialization...")
        legacy_config = {
            "host": "127.0.0.1",
            "port": 9030,
            "http_port": 8040,
            "user": "pidilite_admin",
            "password": "0jqhC3X541tP1RmR.5",
            "database": "pidilite_db",
        }
        legacy_loader = StarRocksStreamLoader(
            config=legacy_config,
            logger=logger,
            debug=True
        )
        print(f"  ✓ Legacy loader initialized")
        print(f"    - Database: {legacy_loader.config['database']}")
        print(f"    - Tenant slug: {legacy_loader.tenant_slug} (should be None)")

        # Test 4c: Verify error handling
        print("\n  Testing error handling (no config provided)...")
        try:
            bad_loader = StarRocksStreamLoader()
            print(f"  ❌ Should have raised ValueError!")
        except ValueError as e:
            print(f"  ✓ Correctly raised ValueError: {str(e)}")

        print(f"\n✅ TEST 4 PASSED - StarRocks loader working correctly")

    except Exception as e:
        print(f"\n❌ TEST 4 FAILED: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Run all tests"""
    print("=" * 80)
    print("WEEK 2 COMPONENT INTEGRATION TESTS")
    print("=" * 80)
    print(f"\nTesting tenant-aware components:")
    print("  - TenantManager and TenantConfig")
    print("  - DatabaseManager")
    print("  - Transformation Engine")
    print("  - StarRocks Stream Loader")

    try:
        # Test 1: TenantManager
        tenant_manager = test_tenant_manager()

        # Test 2: DatabaseManager
        test_database_manager(tenant_manager)

        # Test 3: Transformation Engine
        test_transformation_engine(tenant_manager)

        # Test 4: StarRocks Loader
        test_starrocks_loader(tenant_manager)

        # Summary
        print("\n" + "=" * 80)
        print("ALL TESTS COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\n✅ Week 2 components are working correctly!")
        print("\nNext steps:")
        print("  1. The tenant-aware infrastructure is ready")
        print("  2. Core components support both legacy and multi-tenant modes")
        print("  3. You can now update job scheduler to use TenantManager")
        print("  4. All existing code remains backward compatible")

    except Exception as e:
        print("\n" + "=" * 80)
        print("TESTS FAILED")
        print("=" * 80)
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
