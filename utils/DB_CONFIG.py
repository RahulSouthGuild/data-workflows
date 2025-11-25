"""
StarRocks Database Configuration
Loads connection settings from environment variables
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env.starrocks
env_path = Path(__file__).parent.parent / ".env.starrocks"
if env_path.exists():
    load_dotenv(env_path)

DB_CONFIG = {
    "host": os.getenv("STARROCKS_HOST", "localhost"),
    "port": int(os.getenv("STARROCKS_PORT", 9030)),
    "user": os.getenv("STARROCKS_USER", "datawiz_admin"),
    "password": os.getenv("STARROCKS_PASSWORD", "0jqhC3X541tP1RmR.5"),
    "database": os.getenv("STARROCKS_DATABASE", "datawiz"),
    "charset": "utf8mb4",
    "autocommit": True,
}
