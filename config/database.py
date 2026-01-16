"""
Database connection management for StarRocks.

This module provides both legacy (single-tenant) and new (multi-tenant)
database connection pooling.

Legacy Usage (backward compatible):
    from config.database import create_main_pool, get_connection
    engine = create_main_pool()

Multi-Tenant Usage (new):
    from orchestration.tenant_manager import TenantManager, TenantConfig
    from config.database import DatabaseManager

    tenant_config = tenant_manager.get_tenant("tenant1")
    engine = DatabaseManager.get_engine(tenant_config)
"""

from typing import Optional, Dict, TYPE_CHECKING
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from orchestration.tenant_manager import TenantConfig

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connections and connection pools.

    Supports both legacy single-tenant mode (backward compatible) and
    new multi-tenant mode with separate pools per tenant.
    """

    # Legacy single-tenant engine (backward compatibility)
    _engine: Optional[Engine] = None

    # Multi-tenant engines: tenant_id → Engine mapping
    _tenant_engines: Dict[str, Engine] = {}

    @classmethod
    def create_engine(cls, tenant_config: Optional['TenantConfig'] = None) -> Engine:
        """
        Create SQLAlchemy engine with connection pooling.

        Args:
            tenant_config: Optional TenantConfig object for multi-tenant mode.
                          If None, uses legacy single-tenant mode (from config.settings).

        Returns:
            SQLAlchemy Engine instance

        Examples:
            # Multi-tenant mode (new)
            engine = DatabaseManager.create_engine(tenant_config)

            # Legacy mode (backward compatible)
            engine = DatabaseManager.create_engine()
        """
        # Multi-tenant mode
        if tenant_config is not None:
            tenant_id = tenant_config.tenant_id

            # Return existing engine if already created for this tenant
            if tenant_id in cls._tenant_engines:
                return cls._tenant_engines[tenant_id]

            # Create connection URL from tenant config
            connection_url = (
                f"mysql+pymysql://{tenant_config.database_user}:{tenant_config.database_password}"
                f"@{tenant_config.database_host}:{tenant_config.database_port}/{tenant_config.database_name}"
            )

            # Load pool configuration from shared config
            from pathlib import Path
            import yaml
            pool_config_file = Path(__file__).parent.parent / "configs" / "starrocks" / "connection_pool.yaml"
            if pool_config_file.exists():
                with open(pool_config_file) as f:
                    pool_config = yaml.safe_load(f)
                    pool_settings = pool_config.get('pool', {})
            else:
                # Fallback defaults
                pool_settings = {
                    'pool_size': 10,
                    'max_overflow': 20,
                    'pool_pre_ping': True,
                    'pool_recycle': 3600
                }

            engine = create_engine(
                connection_url,
                poolclass=pool.QueuePool,
                pool_size=pool_settings.get('pool_size', 10),
                max_overflow=pool_settings.get('max_overflow', 20),
                pool_pre_ping=pool_settings.get('pool_pre_ping', True),
                pool_recycle=pool_settings.get('pool_recycle', 3600),
                echo=False,
            )

            cls._tenant_engines[tenant_id] = engine

            logger.info(
                f"[{tenant_config.tenant_slug}] Database engine created: "
                f"{tenant_config.database_host}:{tenant_config.database_port}/{tenant_config.database_name}"
            )
            return engine

        # Legacy single-tenant mode (backward compatibility)
        else:
            if cls._engine is not None:
                return cls._engine

            # Import here to avoid circular dependency
            from config.settings import DB_CONFIG

            connection_url = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
                f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )

            cls._engine = create_engine(
                connection_url,
                poolclass=pool.QueuePool,
                pool_size=DB_CONFIG.get("pool_size", 10),
                max_overflow=DB_CONFIG.get("max_overflow", 20),
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            )

            logger.info(
                f"[LEGACY] Database engine created: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            return cls._engine

    @classmethod
    def get_engine(cls, tenant_config: Optional['TenantConfig'] = None) -> Engine:
        """
        Get existing engine or create new one.

        Args:
            tenant_config: Optional TenantConfig object for multi-tenant mode.
                          If None, uses legacy single-tenant mode.

        Returns:
            SQLAlchemy Engine instance
        """
        if tenant_config is not None:
            # Multi-tenant mode
            tenant_id = tenant_config.tenant_id
            if tenant_id not in cls._tenant_engines:
                return cls.create_engine(tenant_config)
            return cls._tenant_engines[tenant_id]
        else:
            # Legacy single-tenant mode
            if cls._engine is None:
                return cls.create_engine()
            return cls._engine

    @classmethod
    def close_pool(cls, tenant_config: Optional['TenantConfig'] = None) -> None:
        """
        Close database connection pool.

        Args:
            tenant_config: Optional TenantConfig object. If provided, closes only that
                          tenant's pool. If None, closes legacy single-tenant pool or all pools.
        """
        if tenant_config is not None:
            # Close specific tenant pool
            tenant_id = tenant_config.tenant_id
            if tenant_id in cls._tenant_engines:
                cls._tenant_engines[tenant_id].dispose()
                del cls._tenant_engines[tenant_id]
                logger.info(f"[{tenant_config.tenant_slug}] Database connection pool closed")
        else:
            # Legacy mode: close single-tenant pool
            if cls._engine is not None:
                cls._engine.dispose()
                cls._engine = None
                logger.info("[LEGACY] Database connection pool closed")

    @classmethod
    @contextmanager
    def get_connection(cls, tenant_config: Optional['TenantConfig'] = None):
        """
        Context manager for database connections.

        Args:
            tenant_config: Optional TenantConfig object for multi-tenant mode.
        """
        engine = cls.get_engine(tenant_config)
        connection = engine.connect()
        try:
            yield connection
            connection.commit()
        except Exception as e:
            connection.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            connection.close()


# Convenience functions (backward compatibility)
def create_main_pool() -> Engine:
    """Create main database connection pool (legacy single-tenant mode)."""
    return DatabaseManager.create_engine()


def close_db_pool() -> None:
    """Close database connection pool (legacy single-tenant mode)."""
    DatabaseManager.close_pool()


def get_connection():
    """Get database connection context manager (legacy single-tenant mode)."""
    return DatabaseManager.get_connection()
