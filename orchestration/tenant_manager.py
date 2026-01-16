"""
Tenant configuration manager - loads and manages tenant-specific configs.

This module provides the core classes for managing multi-tenant configurations:
- TenantConfig: Represents configuration for a single tenant
- TenantManager: Manages all tenant configurations and orchestration
"""

import yaml
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class TenantConfig:
    """
    Represents configuration for a single tenant.

    This class loads and merges configurations from multiple sources:
    1. configs/shared/default_config.yaml (global defaults)
    2. configs/tenants/{tenant_id}/config.yaml (tenant-specific)
    3. configs/tenants/{tenant_id}/.env (secrets)
    4. configs/tenant_registry.yaml (orchestration metadata)

    Provides easy access to all tenant-specific settings via properties.
    """

    def __init__(
        self,
        tenant_id: str,
        config_path: Path,
        registry_entry: Dict[str, Any],
        shared_defaults: Dict[str, Any]
    ):
        """
        Initialize tenant configuration.

        Args:
            tenant_id: Unique tenant identifier (e.g., "tenant1", "pidilite")
            config_path: Path to tenant config directory (configs/tenants/{tenant_id}/)
            registry_entry: Tenant entry from tenant_registry.yaml
            shared_defaults: Global defaults from configs/shared/default_config.yaml
        """
        self.tenant_id = tenant_id
        self.config_path = config_path
        self.registry_entry = registry_entry
        self.shared_defaults = shared_defaults

        # Load tenant-specific configuration
        self.config = self._load_tenant_config()

        # Load environment variables (secrets)
        self.env = self._load_env()

        # Merge configurations (tenant config overrides shared defaults)
        self.merged_config = self._merge_configs()

        logger.info(f"[{self.tenant_id}] Tenant configuration loaded successfully")

    def _load_tenant_config(self) -> Dict[str, Any]:
        """Load tenant config.yaml file."""
        config_file = self.config_path / "config.yaml"

        if not config_file.exists():
            raise FileNotFoundError(
                f"Tenant config file not found: {config_file}"
            )

        with open(config_file) as f:
            config = yaml.safe_load(f)

        logger.debug(f"[{self.tenant_id}] Loaded tenant config from {config_file}")
        return config

    def _load_env(self) -> Dict[str, str]:
        """
        Load tenant .env file (secrets).

        Tries to use python-dotenv if available, otherwise falls back to
        simple parser.
        """
        env_file = self.config_path / ".env"

        if not env_file.exists():
            logger.warning(
                f"[{self.tenant_id}] .env file not found at {env_file}, "
                "using empty environment"
            )
            return {}

        try:
            from dotenv import dotenv_values
            env_vars = dotenv_values(env_file)
            logger.debug(f"[{self.tenant_id}] Loaded {len(env_vars)} environment variables")
            return dict(env_vars)
        except ImportError:
            # Fallback parser if python-dotenv not installed
            logger.warning(
                f"[{self.tenant_id}] python-dotenv not installed, using fallback parser"
            )
            env_vars = {}
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        env_vars[key.strip()] = value.strip()
            return env_vars

    def _merge_configs(self) -> Dict[str, Any]:
        """
        Merge configurations with proper precedence.

        Merge order (lowest to highest priority):
        1. shared_defaults (global defaults)
        2. self.config (tenant-specific config)

        Returns:
            Merged configuration dictionary
        """
        import copy
        merged = copy.deepcopy(self.shared_defaults)

        # Deep merge tenant config over defaults
        def deep_merge(base: dict, override: dict) -> dict:
            """Recursively merge override into base."""
            result = base.copy()
            for key, value in override.items():
                if (
                    key in result
                    and isinstance(result[key], dict)
                    and isinstance(value, dict)
                ):
                    result[key] = deep_merge(result[key], value)
                else:
                    result[key] = value
            return result

        merged = deep_merge(merged, self.config)
        return merged

    # ========================================================================
    # Property Accessors - Easy access to common configuration values
    # ========================================================================

    @property
    def tenant_slug(self) -> str:
        """Tenant slug (human-readable identifier)."""
        return self.registry_entry.get('tenant_slug', self.tenant_id)

    @property
    def tenant_name(self) -> str:
        """Human-readable tenant name."""
        return self.registry_entry.get('tenant_name', self.tenant_id)

    @property
    def enabled(self) -> bool:
        """Whether this tenant is enabled for processing."""
        return self.registry_entry.get('enabled', False)

    # Database configuration
    @property
    def database_name(self) -> str:
        """StarRocks database name for this tenant."""
        return self.merged_config['database']['database_name']

    @property
    def database_user(self) -> str:
        """StarRocks database user for this tenant."""
        return self.merged_config['database']['user']

    @property
    def database_password(self) -> str:
        """StarRocks database password (from .env)."""
        return self.env.get('DB_PASSWORD', '')

    @property
    def database_host(self) -> str:
        """StarRocks database host."""
        return self.merged_config['database'].get('host', '127.0.0.1')

    @property
    def database_port(self) -> int:
        """StarRocks database port (MySQL protocol)."""
        return self.merged_config['database'].get('port', 9030)

    @property
    def database_http_port(self) -> int:
        """StarRocks HTTP port (for Stream Load)."""
        return self.merged_config['database'].get('http_port', 8040)

    # Path configurations
    @property
    def schema_path(self) -> Path:
        """Path to schema definitions directory."""
        return self.config_path / "schemas"

    @property
    def tables_path(self) -> Path:
        """Path to table schema YAML files."""
        return self.schema_path / "tables"

    @property
    def views_path(self) -> Path:
        """Path to view schema YAML files."""
        return self.schema_path / "views"

    @property
    def matviews_path(self) -> Path:
        """Path to materialized view schema YAML files."""
        return self.schema_path / "matviews"

    @property
    def column_mappings_path(self) -> Path:
        """Path to column mapping YAML files."""
        return self.config_path / "column_mappings"

    @property
    def computed_columns_path(self) -> Path:
        """Path to computed columns JSON file."""
        return self.config_path / "computed_columns.json"

    @property
    def seeds_path(self) -> Path:
        """Path to seed data directory."""
        return self.config_path / "seeds"

    @property
    def business_logic_path(self) -> Path:
        """Path to business logic directory."""
        return self.config_path / "business_logic"

    # Data paths (relative to project root)
    @property
    def data_base_path(self) -> Path:
        """Base data directory for this tenant."""
        base = self.merged_config.get('data_paths', {}).get('base', 'data')
        return Path(base) / self.tenant_slug

    @property
    def data_historical_path(self) -> Path:
        """Historical data directory."""
        return self.data_base_path / "historical"

    @property
    def data_incremental_path(self) -> Path:
        """Incremental data directory."""
        return self.data_base_path / "incremental"

    @property
    def data_temp_path(self) -> Path:
        """Temporary data directory."""
        return self.data_base_path / "temp"

    # Log paths
    @property
    def logs_base_path(self) -> Path:
        """Base log directory for this tenant."""
        base = self.merged_config.get('logging', {}).get('base_path', 'logs')
        return Path(base) / self.tenant_slug

    # Azure storage configuration
    @property
    def storage_provider(self) -> str:
        """Storage provider (azure, aws, gcp, minio, local)."""
        return self.merged_config.get('storage_provider', 'azure')

    @property
    def azure_account_url(self) -> str:
        """Azure Blob Storage account URL (from .env)."""
        return self.env.get('AZURE_ACCOUNT_URL', '')

    @property
    def azure_container_name(self) -> str:
        """Azure Blob Storage container name."""
        return self.merged_config.get('storage_config', {}).get('container_name', '')

    @property
    def azure_sas_token(self) -> str:
        """Azure Blob Storage SAS token (from .env)."""
        return self.env.get('AZURE_SAS_TOKEN', '')

    @property
    def azure_folder_prefix(self) -> str:
        """Azure Blob Storage folder prefix."""
        return self.merged_config.get('storage_config', {}).get('folder_prefix', '')

    # Business rules
    @property
    def business_rules(self) -> Dict[str, Any]:
        """Tenant-specific business rules."""
        return self.merged_config.get('business_rules', {})

    @property
    def date_filter_start(self) -> str:
        """Date filter start for FactInvoiceSecondary."""
        return self.business_rules.get('date_filter_start', '')

    # Scheduler configuration
    @property
    def timezone(self) -> str:
        """Scheduler timezone."""
        return self.merged_config.get('scheduler', {}).get('timezone', 'Asia/Kolkata')

    @property
    def enable_evening_jobs(self) -> bool:
        """Whether to enable evening jobs (dimension sync)."""
        return self.merged_config.get('scheduler', {}).get('enable_evening_jobs', True)

    @property
    def enable_morning_jobs(self) -> bool:
        """Whether to enable morning jobs (fact loads)."""
        return self.merged_config.get('scheduler', {}).get('enable_morning_jobs', True)

    # Observability
    @property
    def observability_service_name(self) -> str:
        """Service name for observability tracing."""
        return self.merged_config.get('observability', {}).get(
            'service_name',
            f'datawiz-{self.tenant_slug}'
        )

    # Feature flags
    @property
    def features(self) -> Dict[str, bool]:
        """Feature flags for this tenant."""
        return self.merged_config.get('features', {})

    @property
    def enable_rls(self) -> bool:
        """Whether Row-Level Security is enabled."""
        return self.features.get('enable_rls', False)

    @property
    def enable_matviews(self) -> bool:
        """Whether materialized views are enabled."""
        return self.features.get('enable_matviews', False)

    @property
    def enable_dd_logic(self) -> bool:
        """Whether distributor dashboard logic is enabled."""
        return self.features.get('enable_dd_logic', False)

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"TenantConfig(tenant_id='{self.tenant_id}', "
            f"tenant_name='{self.tenant_name}', "
            f"database='{self.database_name}', "
            f"enabled={self.enabled})"
        )


class TenantManager:
    """
    Manages all tenant configurations and orchestration.

    This class is responsible for:
    - Loading tenant registry
    - Loading shared defaults
    - Initializing TenantConfig objects for all enabled tenants
    - Providing access to tenant configurations
    - Determining tenant execution order
    """

    def __init__(self, configs_base_path: Path):
        """
        Initialize tenant manager.

        Args:
            configs_base_path: Path to configs directory (usually project_root/configs)
        """
        self.configs_base_path = Path(configs_base_path)

        # Load configurations
        self.registry = self._load_registry()
        self.shared_defaults = self._load_shared_defaults()

        # Initialize tenant configurations
        self.tenants: Dict[str, TenantConfig] = {}
        self._load_tenants()

        logger.info(
            f"TenantManager initialized with {len(self.tenants)} enabled tenant(s)"
        )

    def _load_registry(self) -> Dict[str, Any]:
        """Load tenant_registry.yaml."""
        registry_file = self.configs_base_path / "tenant_registry.yaml"

        if not registry_file.exists():
            raise FileNotFoundError(
                f"Tenant registry not found: {registry_file}"
            )

        with open(registry_file) as f:
            registry = yaml.safe_load(f)

        logger.info(f"Loaded tenant registry from {registry_file}")
        return registry

    def _load_shared_defaults(self) -> Dict[str, Any]:
        """Load shared default configuration."""
        defaults_file = self.configs_base_path / "shared" / "default_config.yaml"

        if not defaults_file.exists():
            logger.warning(
                f"Shared defaults not found: {defaults_file}, using empty defaults"
            )
            return {}

        with open(defaults_file) as f:
            defaults = yaml.safe_load(f)

        logger.info(f"Loaded shared defaults from {defaults_file}")
        return defaults

    def _load_tenants(self):
        """Load all enabled tenants from registry."""
        for tenant_info in self.registry.get('tenants', []):
            if tenant_info.get('enabled', False):
                tenant_id = tenant_info['tenant_id']
                tenant_slug = tenant_info.get('tenant_slug', tenant_id)

                # Path can be either tenant_id or tenant_slug
                # Try tenant_slug first (e.g., "pidilite"), then tenant_id
                config_path = self.configs_base_path / "tenants" / tenant_slug
                if not config_path.exists():
                    config_path = self.configs_base_path / "tenants" / tenant_id

                if not config_path.exists():
                    logger.warning(
                        f"Config directory not found for tenant {tenant_id} "
                        f"(tried {tenant_slug} and {tenant_id}), skipping"
                    )
                    continue

                try:
                    tenant_config = TenantConfig(
                        tenant_id=tenant_id,
                        config_path=config_path,
                        registry_entry=tenant_info,
                        shared_defaults=self.shared_defaults
                    )
                    self.tenants[tenant_id] = tenant_config

                    logger.info(
                        f"Loaded tenant: {tenant_config.tenant_name} ({tenant_id})"
                    )

                except Exception as e:
                    logger.error(
                        f"Failed to load tenant {tenant_id}: {str(e)}",
                        exc_info=True
                    )

    def get_tenant(self, tenant_id: str) -> Optional[TenantConfig]:
        """
        Get configuration for a specific tenant.

        Args:
            tenant_id: Tenant UUID

        Returns:
            TenantConfig object or None if not found
        """
        return self.tenants.get(tenant_id)

    def get_tenant_by_slug(self, tenant_slug: str) -> Optional[TenantConfig]:
        """
        Get configuration for a specific tenant by slug.

        Args:
            tenant_slug: Tenant slug (e.g., "pidilite", "uthra-global")

        Returns:
            TenantConfig object or None if not found
        """
        for tenant in self.tenants.values():
            if tenant.tenant_slug == tenant_slug:
                return tenant
        return None

    def get_all_enabled_tenants(self) -> List[TenantConfig]:
        """
        Get all enabled tenant configurations, sorted by priority.

        Returns:
            List of TenantConfig objects sorted by schedule_priority
            (lower priority number = higher priority)
        """
        tenants = list(self.tenants.values())

        # Sort by schedule_priority from registry
        tenants.sort(
            key=lambda t: t.registry_entry.get('schedule_priority', 999)
        )

        return tenants

    @property
    def global_config(self) -> Dict[str, Any]:
        """Get global configuration from registry."""
        return self.registry.get('global_config', {})

    @property
    def max_concurrent_tenants(self) -> int:
        """Maximum number of tenants to process concurrently."""
        return self.global_config.get('max_concurrent_tenants', 1)

    @property
    def tenant_timeout(self) -> int:
        """Maximum time per tenant in seconds."""
        return self.global_config.get('tenant_timeout', 7200)

    @property
    def fail_fast(self) -> bool:
        """Whether to stop processing on first tenant failure."""
        return self.global_config.get('fail_fast', False)

    def __repr__(self) -> str:
        """String representation."""
        tenant_names = [t.tenant_name for t in self.tenants.values()]
        return (
            f"TenantManager(tenants={len(self.tenants)}, "
            f"enabled=[{', '.join(tenant_names)}])"
        )
