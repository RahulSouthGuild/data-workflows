"""
Database connection management for StarRocks.
Extract from incremental_utils.py: create_main_pool, close_db_pool
"""

from typing import Optional
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, pool
from sqlalchemy.engine import Engine
from config.settings import DB_CONFIG

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and connection pools."""

    _engine: Optional[Engine] = None

    @classmethod
    def create_engine(cls) -> Engine:
        """Create SQLAlchemy engine with connection pooling."""
        if cls._engine is not None:
            return cls._engine

        connection_url = (
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        cls._engine = create_engine(
            connection_url,
            poolclass=pool.QueuePool,
            pool_size=DB_CONFIG["pool_size"],
            max_overflow=DB_CONFIG["max_overflow"],
            pool_pre_ping=True,  # Verify connections before using
            pool_recycle=3600,   # Recycle connections after 1 hour
            echo=False,
        )

        logger.info(
            f"Database engine created: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        return cls._engine

    @classmethod
    def get_engine(cls) -> Engine:
        """Get existing engine or create new one."""
        if cls._engine is None:
            return cls.create_engine()
        return cls._engine

    @classmethod
    def close_pool(cls) -> None:
        """Close database connection pool."""
        if cls._engine is not None:
            cls._engine.dispose()
            cls._engine = None
            logger.info("Database connection pool closed")

    @classmethod
    @contextmanager
    def get_connection(cls):
        """Context manager for database connections."""
        engine = cls.get_engine()
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


# Convenience functions
def create_main_pool() -> Engine:
    """Create main database connection pool."""
    return DatabaseManager.create_engine()


def close_db_pool() -> None:
    """Close database connection pool."""
    DatabaseManager.close_pool()


def get_connection():
    """Get database connection context manager."""
    return DatabaseManager.get_connection()
