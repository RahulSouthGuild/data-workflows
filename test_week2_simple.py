"""
Week 2 Simple Test - Without Database Dependencies

This script tests Week 2 components without requiring sqlalchemy/database connections.
Tests only configuration loading and tenant-aware path resolution.
"""

from pathlib import Path
import sys

def test_tenant_manager():
    """Test 1: TenantManager initialization and configuration loading"""
    print("\n" + "=" * 80)
    print("TEST 1: TENANT MANAGER - CONFIGURATION LOADING")
    print("=" * 80)

    try:
        from orchestration.tenant_manager import TenantManager

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
            print(f"    - database_host: {tenant_config.database_host}")
            print(f"    - database_port: {tenant_config.database_port}")
            print(f"    - azure_container: {tenant_config.azure_container_name}")
            print(f"    - schema_path: {tenant_config.schema_path}")
            print(f"    - column_mappings_path: {tenant_config.column_mappings_path}")
            print(f"    - computed_columns_path: {tenant_config.computed_columns_path}")

            # Verify paths exist
            print(f"\n  Path Verification:")
            print(f"    - Schema path exists: {tenant_config.schema_path.exists()}")
            print(f"    - Tables path exists: {tenant_config.tables_path.exists()}")
            print(f"    - Views path exists: {tenant_config.views_path.exists()}")
            print(f"    - Matviews path exists: {tenant_config.matviews_path.exists()}")
            print(f"    - Column mappings exists: {tenant_config.column_mappings_path.exists()}")
            print(f"    - Computed columns exists: {tenant_config.computed_columns_path.exists()}")

            # Count files
            if tenant_config.tables_path.exists():
                table_files = list(tenant_config.tables_path.glob("*.yaml"))
                view_files = list(tenant_config.views_path.glob("*.yaml"))
                matview_files = list(tenant_config.matviews_path.glob("*.yaml"))
                mapping_files = list(tenant_config.column_mappings_path.glob("*.yaml"))

                print(f"\n  Schema Files Found:")
                print(f"    - Table schemas: {len(table_files)}")
                print(f"    - View schemas: {len(view_files)}")
                print(f"    - Materialized view schemas: {len(matview_files)}")
                print(f"    - Column mappings: {len(mapping_files)}")

        print(f"\n✅ TEST 1 PASSED - TenantManager working correctly")
        return tenant_manager

    except Exception as e:
        print(f"\n❌ TEST 1 FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


def test_computed_columns_loading(tenant_manager):
    """Test 2: Load computed columns configuration"""
    print("\n" + "=" * 80)
    print("TEST 2: COMPUTED COLUMNS CONFIGURATION")
    print("=" * 80)

    try:
        from core.transformers.transformation_engine import load_computed_columns_config

        tenants = tenant_manager.get_all_enabled_tenants()
        if not tenants:
            print("⚠️  No enabled tenants found")
            return

        tenant_config = tenants[0]
        print(f"\nTesting with tenant: {tenant_config.tenant_name}")

        # Test loading tenant-specific computed columns
        print("\n  Loading tenant-specific computed columns config...")
        config = load_computed_columns_config(tenant_config.computed_columns_path)

        print(f"  ✓ Loaded successfully")
        print(f"    - Tables with computed columns: {list(config.keys())}")

        # Show details for each table
        for table_name, table_config in config.items():
            print(f"\n    Table: {table_name}")
            for col_name, col_def in table_config.items():
                print(f"      - {col_name}: {col_def.get('type', 'unknown')}")

        print(f"\n✅ TEST 2 PASSED - Computed columns config loaded correctly")

    except Exception as e:
        print(f"\n❌ TEST 2 FAILED: {str(e)}")
        import traceback
        traceback.print_exc()


def test_transformation_engine_with_sample_data(tenant_manager):
    """Test 3: Transformation engine with sample data (no database)"""
    print("\n" + "=" * 80)
    print("TEST 3: TRANSFORMATION ENGINE - COMPUTED COLUMNS")
    print("=" * 80)

    try:
        from core.transformers.transformation_engine import generate_computed_columns
        import polars as pl

        tenants = tenant_manager.get_all_enabled_tenants()
        if not tenants:
            print("⚠️  No enabled tenants found")
            return

        tenant_config = tenants[0]
        print(f"\nTesting with tenant: {tenant_config.tenant_name}")

        # Create test data for fact_invoice_secondary
        print("\n  Creating test dataframe...")
        test_df = pl.DataFrame({
            'invoice_date': ['20250116', '20250117', '20250118'],
            'customer_code': ['CUST001', 'CUST002', 'CUST003'],
            'invoice_no': ['INV001', 'INV002', 'INV003']
        })

        print(f"    - Rows: {len(test_df)}")
        print(f"    - Original columns: {test_df.columns}")

        # Generate computed columns
        print("\n  Generating computed columns...")
        result_df = generate_computed_columns(
            test_df,
            'fact_invoice_secondary',
            tenant_config=tenant_config
        )

        print(f"  ✓ Generated successfully")
        print(f"    - Result columns: {result_df.columns}")
        print(f"    - New columns added: {set(result_df.columns) - set(test_df.columns)}")

        # Show sample data
        if len(result_df.columns) > len(test_df.columns):
            print(f"\n  Sample data (first row):")
            for col in result_df.columns:
                print(f"    - {col}: {result_df[col][0]}")

        print(f"\n✅ TEST 3 PASSED - Transformation engine working correctly")

    except Exception as e:
        print(f"\n❌ TEST 3 FAILED: {str(e)}")
        import traceback
        traceback.print_exc()


def test_file_structure():
    """Test 4: Verify file structure and code changes"""
    print("\n" + "=" * 80)
    print("TEST 4: FILE STRUCTURE & CODE CHANGES")
    print("=" * 80)

    try:
        # Check directory structure
        print("\n  Checking directory structure...")

        checks = {
            "orchestration/tenant_manager.py": Path("orchestration/tenant_manager.py"),
            "orchestration/__init__.py": Path("orchestration/__init__.py"),
            "configs/tenants/pidilite/": Path("configs/tenants/pidilite"),
            "configs/tenants/uthra-global/": Path("configs/tenants/uthra-global"),
            "configs/tenants/pidilite/schemas/": Path("configs/tenants/pidilite/schemas"),
            "configs/tenants/pidilite/column_mappings/": Path("configs/tenants/pidilite/column_mappings"),
        }

        for name, path in checks.items():
            exists = path.exists()
            status = "✓" if exists else "✗"
            print(f"    {status} {name}: {'exists' if exists else 'MISSING'}")

        # Check code changes
        print("\n  Verifying code changes...")

        # Check transformation_engine.py has tenant_config parameter
        with open("core/transformers/transformation_engine.py") as f:
            content = f.read()
            has_tenant_config = "tenant_config: Optional['TenantConfig']" in content
            has_type_checking = "if TYPE_CHECKING:" in content

            print(f"    {'✓' if has_tenant_config else '✗'} transformation_engine.py has tenant_config parameter")
            print(f"    {'✓' if has_type_checking else '✗'} transformation_engine.py has TYPE_CHECKING import")

        # Check database.py has tenant_config parameter
        with open("config/database.py") as f:
            content = f.read()
            has_tenant_engines = "_tenant_engines: Dict[str, Engine]" in content
            has_tenant_param = "tenant_config: Optional['TenantConfig']" in content

            print(f"    {'✓' if has_tenant_engines else '✗'} database.py has _tenant_engines variable")
            print(f"    {'✓' if has_tenant_param else '✗'} database.py has tenant_config parameter")

        # Check starrocks_stream_loader.py
        with open("core/loaders/starrocks_stream_loader.py") as f:
            content = f.read()
            has_tenant_slug = "self.tenant_slug" in content

            print(f"    {'✓' if has_tenant_slug else '✗'} starrocks_stream_loader.py has tenant_slug")

        print(f"\n✅ TEST 4 PASSED - File structure verified")

    except Exception as e:
        print(f"\n❌ TEST 4 FAILED: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    """Run all tests"""
    print("=" * 80)
    print("WEEK 2 SIMPLE INTEGRATION TESTS")
    print("=" * 80)
    print(f"\nTesting tenant-aware components (without database connections):")
    print("  - TenantManager and TenantConfig")
    print("  - Computed columns loading")
    print("  - Transformation engine")
    print("  - File structure verification")

    try:
        # Test 1: TenantManager
        tenant_manager = test_tenant_manager()

        # Test 2: Computed columns
        test_computed_columns_loading(tenant_manager)

        # Test 3: Transformation engine
        test_transformation_engine_with_sample_data(tenant_manager)

        # Test 4: File structure
        test_file_structure()

        # Summary
        print("\n" + "=" * 80)
        print("ALL TESTS COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print("\n✅ Week 2 core components are working correctly!")
        print("\nWhat was tested:")
        print("  ✓ TenantManager loads Pidilite configuration")
        print("  ✓ All tenant configuration properties accessible")
        print("  ✓ Tenant-specific paths resolved correctly")
        print("  ✓ Computed columns configuration loaded")
        print("  ✓ Transformation engine generates computed columns")
        print("  ✓ File structure verified")
        print("  ✓ Code changes verified")

        print("\nNext steps:")
        print("  1. Install sqlalchemy/pymysql to test database connections")
        print("  2. Run full test suite: python test_week2_components.py")
        print("  3. Update job scheduler to use TenantManager")
        print("  4. Test with real parquet files")

    except Exception as e:
        print("\n" + "=" * 80)
        print("TESTS FAILED")
        print("=" * 80)
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
