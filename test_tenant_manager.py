"""
Test script to verify TenantManager implementation.

This script tests:
1. Loading tenant registry
2. Loading shared defaults
3. Initializing TenantConfig for enabled tenants
4. Accessing tenant configuration properties
"""

from pathlib import Path
from orchestration.tenant_manager import TenantManager
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    print("=" * 80)
    print("TENANT MANAGER TEST")
    print("=" * 80)

    # Initialize tenant manager
    configs_path = Path(__file__).parent / "configs"
    print(f"\nInitializing TenantManager from: {configs_path}")

    tenant_manager = TenantManager(configs_path)

    print(f"\n{tenant_manager}")

    # Global config
    print(f"\nGlobal Configuration:")
    print(f"  max_concurrent_tenants: {tenant_manager.max_concurrent_tenants}")
    print(f"  tenant_timeout: {tenant_manager.tenant_timeout}s")
    print(f"  fail_fast: {tenant_manager.fail_fast}")

    # Get all enabled tenants
    enabled_tenants = tenant_manager.get_all_enabled_tenants()
    print(f"\nEnabled Tenants (sorted by priority): {len(enabled_tenants)}")

    for tenant_config in enabled_tenants:
        print(f"\n{'=' * 80}")
        print(f"Tenant: {tenant_config.tenant_name} ({tenant_config.tenant_id})")
        print(f"{'=' * 80}")

        # Basic info
        print(f"\nBasic Info:")
        print(f"  tenant_slug: {tenant_config.tenant_slug}")
        print(f"  enabled: {tenant_config.enabled}")
        print(f"  config_path: {tenant_config.config_path}")

        # Database configuration
        print(f"\nDatabase Configuration:")
        print(f"  database_name: {tenant_config.database_name}")
        print(f"  database_user: {tenant_config.database_user}")
        print(f"  database_host: {tenant_config.database_host}")
        print(f"  database_port: {tenant_config.database_port}")
        print(f"  database_http_port: {tenant_config.database_http_port}")
        print(f"  database_password: {'*' * len(tenant_config.database_password)} (hidden)")

        # Storage configuration
        print(f"\nStorage Configuration:")
        print(f"  storage_provider: {tenant_config.storage_provider}")
        print(f"  azure_container_name: {tenant_config.azure_container_name}")
        print(f"  azure_folder_prefix: {tenant_config.azure_folder_prefix}")
        print(f"  azure_account_url: {tenant_config.azure_account_url}")
        print(f"  azure_sas_token: {'*' * min(20, len(tenant_config.azure_sas_token))}... (hidden)")

        # Paths
        print(f"\nFile Paths:")
        print(f"  schema_path: {tenant_config.schema_path}")
        print(f"  tables_path: {tenant_config.tables_path}")
        print(f"  views_path: {tenant_config.views_path}")
        print(f"  matviews_path: {tenant_config.matviews_path}")
        print(f"  column_mappings_path: {tenant_config.column_mappings_path}")
        print(f"  computed_columns_path: {tenant_config.computed_columns_path}")
        print(f"  seeds_path: {tenant_config.seeds_path}")

        # Data paths
        print(f"\nData Paths:")
        print(f"  data_base_path: {tenant_config.data_base_path}")
        print(f"  data_historical_path: {tenant_config.data_historical_path}")
        print(f"  data_incremental_path: {tenant_config.data_incremental_path}")
        print(f"  data_temp_path: {tenant_config.data_temp_path}")

        # Log paths
        print(f"\nLog Paths:")
        print(f"  logs_base_path: {tenant_config.logs_base_path}")

        # Business rules
        print(f"\nBusiness Rules:")
        print(f"  date_filter_start: {tenant_config.date_filter_start}")
        print(f"  business_rules: {tenant_config.business_rules}")

        # Scheduler
        print(f"\nScheduler Configuration:")
        print(f"  timezone: {tenant_config.timezone}")
        print(f"  enable_evening_jobs: {tenant_config.enable_evening_jobs}")
        print(f"  enable_morning_jobs: {tenant_config.enable_morning_jobs}")

        # Observability
        print(f"\nObservability:")
        print(f"  service_name: {tenant_config.observability_service_name}")

        # Feature flags
        print(f"\nFeature Flags:")
        print(f"  enable_rls: {tenant_config.enable_rls}")
        print(f"  enable_matviews: {tenant_config.enable_matviews}")
        print(f"  enable_dd_logic: {tenant_config.enable_dd_logic}")

        # Verify schema files exist
        print(f"\nSchema Files Verification:")
        table_files = list(tenant_config.tables_path.glob("*.yaml"))
        print(f"  Table schemas: {len(table_files)} files")

        view_files = list(tenant_config.views_path.glob("*.yaml"))
        print(f"  View schemas: {len(view_files)} files")

        matview_files = list(tenant_config.matviews_path.glob("*.yaml"))
        print(f"  Materialized view schemas: {len(matview_files)} files")

        mapping_files = list(tenant_config.column_mappings_path.glob("*.yaml"))
        print(f"  Column mappings: {len(mapping_files)} files")

    print(f"\n{'=' * 80}")
    print("TEST COMPLETED SUCCESSFULLY")
    print(f"{'=' * 80}")

if __name__ == "__main__":
    main()
