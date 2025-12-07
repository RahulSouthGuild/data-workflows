"""
Configuration Utilities

Centralized configuration management for data pipeline jobs
"""

import os
from typing import Dict
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / ".env.starrocks"
if env_path.exists():
    load_dotenv(env_path)


class Config:
    """Centralized configuration for pipeline jobs"""

    # StarRocks Configuration
    STARROCKS_HOST = os.getenv("STARROCKS_HOST", "localhost")
    STARROCKS_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
    STARROCKS_HTTP_PORT = int(os.getenv("STARROCKS_HTTP_PORT", "8040"))
    STARROCKS_USER = os.getenv("STARROCKS_USER", "datawiz_admin")
    STARROCKS_PASSWORD = os.getenv("STARROCKS_PASSWORD", "0jqhC3X541tP1RmR.5")
    STARROCKS_DATABASE = os.getenv("STARROCKS_DATABASE", "datawiz")

    # Azure Configuration
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "synapsedataprod")
    AZURE_SAS_TOKEN = os.getenv("AZURE_SAS_TOKEN", "")

    # Processing Configuration
    CHUNK_SIZE = 100000  # Records per chunk for bulk operations
    STREAM_LOAD_TIMEOUT = 1800  # 30 minutes
    MAX_ERROR_RATIO = 0.1  # 10% error tolerance
    MAX_RETRIES = 3
    RETRY_DELAY = 2

    # Data Paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    DATA_INCREMENTAL_DIR = DATA_DIR / "data_incremental"
    DATA_INCREMENTAL_RAW = DATA_INCREMENTAL_DIR / "incremental"
    DATA_INCREMENTAL_PARQUETS_RAW = DATA_INCREMENTAL_DIR / "raw_parquets"
    DATA_INCREMENTAL_PARQUETS_CLEANED = DATA_INCREMENTAL_DIR / "cleaned_parquets"

    # Schema Configuration
    # SCHEMA_PATHS = [
    #     Path("/schemas/files_schema.json"),  # Local directory
    #     PROJECT_ROOT / "schemas" / "files_schema.json",
    #     PROJECT_ROOT / "pidilite-pipeline-utils" / "schemas" / "files_schema.json",
    # ]

    @classmethod
    def get_db_config(cls) -> Dict[str, any]:
        """Get StarRocks database configuration as dictionary

        Returns:
            Dictionary with database connection parameters
        """
        return {
            "host": cls.STARROCKS_HOST,
            "port": cls.STARROCKS_PORT,
            "http_port": cls.STARROCKS_HTTP_PORT,
            "user": cls.STARROCKS_USER,
            "password": cls.STARROCKS_PASSWORD,
            "database": cls.STARROCKS_DATABASE,
            "charset": "utf8mb4",
            "autocommit": True,
        }

    @classmethod
    def get_azure_config(cls) -> Dict[str, str]:
        """Get Azure Blob Storage configuration

        Returns:
            Dictionary with Azure connection parameters
        """
        # Extract account URL from connection string
        # Format: https://accountname.blob.core.windows.net
        conn_str = cls.AZURE_STORAGE_CONNECTION_STRING
        if not conn_str:
            account_url = ""
        else:
            # Connection string is just the account URL in this case
            account_url = conn_str if conn_str.startswith("https://") else conn_str

        return {
            "account_url": account_url,
            "sas_token": cls.AZURE_SAS_TOKEN,
            "container_name": cls.AZURE_CONTAINER_NAME,
        }

    @classmethod
    def ensure_directories(cls):
        """Ensure all required directories exist"""
        for directory in [
            cls.DATA_INCREMENTAL_DIR,
            cls.DATA_INCREMENTAL_RAW,
            cls.DATA_INCREMENTAL_PARQUETS_RAW,
            cls.DATA_INCREMENTAL_PARQUETS_CLEANED,
        ]:
            directory.mkdir(parents=True, exist_ok=True)


# Constants for dimension table configuration
DIMENSION_TABLES = {
    "DimHierarchy": "Incremental/DimHierarchy/LatestData/",
    "DimDealerMaster": "Incremental/DimDealer_MS/LatestData/",
    "DimCustomerMaster": "Incremental/DimCustomerMaster/LatestData/",
    "DimMaterial": "Incremental/DimMaterial/LatestData/",
}

# Service names for observability
DAILY_DIMENSION_INCREMENTAL_SERVICE_NAME = "daily-dimension-incremental"
RLS_MAPPER_SERVICE_NAME = "rls-mapper"

# Log separators
LOG_SEPARATOR = "=" * 100
