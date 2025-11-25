"""
Centralized configuration management for Pidilite DataWiz.
Consolidates DB_CONFIG, AZURE_CONFIG, MONGODB_URI from incremental_utils.py
"""

import os
from pathlib import Path
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database Configuration (StarRocks)
DB_CONFIG: Dict[str, Any] = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "9030")),
    "database": os.getenv("DB_NAME", "datawiz"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "pool_size": int(os.getenv("DB_POOL_SIZE", "10")),
    "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "20")),
}

# Azure Blob Storage Configuration
AZURE_CONFIG: Dict[str, Any] = {
    "connection_string": os.getenv("AZURE_STORAGE_CONNECTION_STRING", ""),
    "container_name": os.getenv("AZURE_CONTAINER_NAME", ""),
    "account_name": os.getenv("AZURE_BLOB_ACCOUNT_NAME", ""),
    "account_key": os.getenv("AZURE_BLOB_ACCOUNT_KEY", ""),
}

# MongoDB Configuration
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "pidilite")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "business_constants")

# Email Configuration
EMAIL_CONFIG: Dict[str, Any] = {
    "smtp_host": os.getenv("SMTP_HOST", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", "587")),
    "smtp_user": os.getenv("SMTP_USER", ""),
    "smtp_password": os.getenv("SMTP_PASSWORD", ""),
    "use_tls": os.getenv("SMTP_USE_TLS", "true").lower() == "true",
    "from_email": os.getenv("EMAIL_FROM", "noreply@pidilite.com"),
    "recipients": os.getenv("EMAIL_RECIPIENTS", "").split(","),
}

# Observability Configuration
SIGNOZ_ENDPOINT = os.getenv("SIGNOZ_ENDPOINT", "http://localhost:4317")
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "pidilite-datawiz")
ENABLE_TRACING = os.getenv("ENABLE_TRACING", "true").lower() == "true"
ENABLE_METRICS = os.getenv("ENABLE_METRICS", "true").lower() == "true"

# Scheduler Configuration
SCHEDULER_TIMEZONE = os.getenv("SCHEDULER_TIMEZONE", "Asia/Kolkata")
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "true").lower() == "true"

# Data Paths
DATA_HISTORICAL_PATH = Path(
    os.getenv("DATA_HISTORICAL_PATH", str(DATA_DIR / "data_historical"))
)
DATA_INCREMENTAL_PATH = Path(
    os.getenv("DATA_INCREMENTAL_PATH", str(DATA_DIR / "data_incremental"))
)
TEMP_PATH = Path(os.getenv("TEMP_PATH", str(DATA_DIR / "temp")))

# Maintenance Mode
MAINTENANCE_MODE = os.getenv("MAINTENANCE_MODE", "false").lower() == "true"

# Backup Configuration
BACKUP_ENABLED = os.getenv("BACKUP_ENABLED", "true").lower() == "true"
BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", "30"))
BACKUP_PATH = Path(
    os.getenv("BACKUP_PATH", str(BASE_DIR / "backups"))
)


def validate_config() -> None:
    """Validate that required configuration is present."""
    required_configs = {
        "DB_HOST": DB_CONFIG["host"],
        "DB_NAME": DB_CONFIG["database"],
        "AZURE_STORAGE_CONNECTION_STRING": AZURE_CONFIG["connection_string"],
    }

    missing = [key for key, value in required_configs.items() if not value]

    if missing:
        raise ValueError(
            f"Missing required configuration: {', '.join(missing)}. "
            "Please check your .env file."
        )


if __name__ == "__main__":
    validate_config()
    print("✓ Configuration validated successfully")
