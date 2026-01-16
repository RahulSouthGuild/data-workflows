"""
Tenant-Aware Table Manager
Manages database objects (tables, views, materialized views) for specific tenants.
"""

import sys
import time
import logging
import yaml
import pymysql
from pathlib import Path
from typing import List, Dict, Optional
from colorama import init, Fore, Style

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from orchestration.tenant_manager import TenantConfig

# Initialize colorama
init(autoreset=True)

# Configure logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "tenant_table_manager.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


class TenantTableManager:
    """Manages database objects for a specific tenant"""

    def __init__(self, tenant_config: TenantConfig):
        """
        Initialize tenant table manager.

        Args:
            tenant_config: TenantConfig object with tenant-specific settings
        """
        self.tenant_config = tenant_config
        self.connection = None

    def connect(self) -> pymysql.Connection:
        """Establish connection to tenant-specific database"""
        try:
            self.print_info(f"🔌 Connecting to {self.tenant_config.tenant_name} database...", Fore.CYAN)
            logger.info(
                f"Connecting to {self.tenant_config.database_name} "
                f"at {self.tenant_config.database_host}:{self.tenant_config.database_port}"
            )

            self.connection = pymysql.connect(
                host=self.tenant_config.database_host,
                port=self.tenant_config.database_port,
                user=self.tenant_config.database_user,
                password=self.tenant_config.database_password,
                database=self.tenant_config.database_name,
                charset="utf8mb4",
                autocommit=True,
            )

            self.print_success(f"Connected to database '{self.tenant_config.database_name}'")
            logger.info(f"Successfully connected to {self.tenant_config.database_name}")
            return self.connection

        except Exception as e:
            self.print_error(f"Failed to connect to database: {e}")
            logger.error(f"Connection error: {e}", exc_info=True)
            raise

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info(f"Disconnected from {self.tenant_config.database_name}")

    @staticmethod
    def print_success(message: str):
        """Print success message"""
        print(f"{Style.BRIGHT}{Fore.GREEN}✅ {message}{Style.RESET_ALL}")

    @staticmethod
    def print_error(message: str):
        """Print error message"""
        print(f"{Style.BRIGHT}{Fore.RED}❌ {message}{Style.RESET_ALL}")

    @staticmethod
    def print_warning(message: str):
        """Print warning message"""
        print(f"{Style.BRIGHT}{Fore.YELLOW}⚠️  {message}{Style.RESET_ALL}")

    @staticmethod
    def print_info(message: str, color=Fore.CYAN):
        """Print info message"""
        print(f"{Style.BRIGHT}{color}{message}{Style.RESET_ALL}")

    def _load_yaml_schemas(self, schema_dir: Path, object_type: str) -> List[Dict]:
        """
        Load all YAML schema files from a directory.

        Args:
            schema_dir: Path to directory containing YAML schema files
            object_type: Type of object (TABLE, VIEW, MATVIEW)

        Returns:
            List of schema dictionaries sorted by order
        """
        if not schema_dir.exists():
            logger.warning(f"{object_type} schema directory not found: {schema_dir}")
            return []

        schemas = []
        for yaml_file in sorted(schema_dir.glob("*.yaml")):
            try:
                with open(yaml_file, "r") as f:
                    schema = yaml.safe_load(f)

                # Ensure required fields
                if not schema.get("name") or not schema.get("sql"):
                    logger.warning(f"Skipping {yaml_file}: missing 'name' or 'sql' field")
                    continue

                # Set defaults
                schema.setdefault("type", object_type)
                schema.setdefault("order", 999)
                schema.setdefault("comments", {})

                schemas.append(schema)
                logger.debug(f"Loaded {object_type} schema: {schema['name']} from {yaml_file.name}")

            except Exception as e:
                logger.error(f"Error loading {yaml_file}: {e}", exc_info=True)
                self.print_warning(f"Skipping {yaml_file.name}: {e}")

        # Sort by order
        schemas.sort(key=lambda x: x.get("order", 999))
        logger.info(f"Loaded {len(schemas)} {object_type} schema(s) for {self.tenant_config.tenant_name}")

        return schemas

    def load_table_schemas(self) -> List[Dict]:
        """
        Load all table schemas from tenant YAML files.

        Returns:
            List of table schema dictionaries sorted by order
        """
        return self._load_yaml_schemas(self.tenant_config.tables_path, "TABLE")

    def load_view_schemas(self) -> List[Dict]:
        """
        Load all view schemas from tenant YAML files.

        Returns:
            List of view schema dictionaries sorted by order
        """
        return self._load_yaml_schemas(self.tenant_config.views_path, "VIEW")

    def load_matview_schemas(self) -> List[Dict]:
        """
        Load all materialized view schemas from tenant YAML files.

        Returns:
            List of materialized view schema dictionaries sorted by order
        """
        return self._load_yaml_schemas(self.tenant_config.matviews_path, "MATVIEW")

    def get_all_schemas(self) -> Dict[str, List[Dict]]:
        """
        Get all schemas organized by type.

        Returns:
            Dictionary with keys 'tables', 'views', 'matviews'
        """
        return {
            "tables": self.load_table_schemas(),
            "views": self.load_view_schemas(),
            "matviews": self.load_matview_schemas(),
        }

    def execute_query(self, query: str, description: str = "", max_retries: int = 3) -> bool:
        """Execute SQL query with retry logic"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            retries = 0

            while retries < max_retries:
                try:
                    logger.debug(f"Executing: {query[:100]}...")
                    cursor.execute(query)
                    self.connection.commit()
                    logger.info(f"Successfully executed: {description}")
                    return True

                except pymysql.err.OperationalError as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(
                            f"Failed to execute {description} after {max_retries} retries: {e}"
                        )
                        self.print_error(f"Failed to execute {description}: {e}")
                        return False

                    self.print_warning(f"Retrying {description} ({retries}/{max_retries})...")
                    logger.warning(f"Retry {retries}/{max_retries} for {description}: {e}")
                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Error executing {description}: {e}", exc_info=True)
                    self.print_error(f"Error executing {description}: {e}")
                    return False

        finally:
            if cursor:
                cursor.close()

    def create_object(self, schema: Dict) -> bool:
        """
        Create database object (table/view/matview) from YAML schema.

        Args:
            schema: Schema dictionary from YAML file

        Returns:
            True if successful, False otherwise
        """
        try:
            name = schema["name"]
            object_type = schema.get("type", "TABLE")
            sql = schema["sql"]

            # Determine drop statement based on type
            if object_type == "VIEW":
                drop_sql = f"DROP VIEW IF EXISTS {name}"
                icon = "👁️"
                color = Fore.CYAN
            elif object_type == "MATVIEW":
                drop_sql = f"DROP MATERIALIZED VIEW IF EXISTS {name}"
                icon = "⚡"
                color = Fore.MAGENTA
            else:
                drop_sql = f"DROP TABLE IF EXISTS {name}"
                icon = "📊"
                color = Fore.BLUE

            self.print_info(f"{icon} Creating {object_type.lower()} {name}...", color)
            logger.info(f"Creating {object_type}: {name}")

            # Drop existing object
            if not self.execute_query(drop_sql, f"drop {object_type.lower()} {name}"):
                return False

            # Create object
            if not self.execute_query(sql, f"create {object_type.lower()} {name}"):
                return False

            # Log comments (StarRocks doesn't support ALTER to add comments, they must be inline)
            comments = schema.get("comments", {})
            if comments.get("table"):
                logger.info(f"Table comment for {name}: {comments['table']}")

            if comments.get("columns"):
                for column, comment in comments["columns"].items():
                    logger.info(f"Column comment for {column} in {name}: {comment}")

            self.print_success(f"Successfully created {object_type.lower()} {name}")
            logger.info(f"{object_type} {name} created successfully")
            return True

        except Exception as e:
            logger.error(f"Error creating {schema.get('name', 'unknown')}: {e}", exc_info=True)
            self.print_error(f"Error creating {schema.get('name', 'unknown')}: {e}")
            return False

    def drop_object(self, name: str, object_type: str = "TABLE") -> bool:
        """
        Drop database object (table/view/matview).

        Args:
            name: Name of the object to drop
            object_type: Type of object ('TABLE', 'VIEW', 'MATVIEW')

        Returns:
            True if successful, False otherwise
        """
        try:
            if object_type == "VIEW":
                query = f"DROP VIEW IF EXISTS {name}"
            elif object_type == "MATVIEW":
                query = f"DROP MATERIALIZED VIEW IF EXISTS {name}"
            else:
                query = f"DROP TABLE IF EXISTS {name}"

            if self.execute_query(query, f"drop {object_type.lower()} {name}"):
                self.print_info(f"🗑️  Dropped {object_type.lower()} {name}", Fore.YELLOW)
                logger.info(f"Dropped {object_type.lower()}: {name}")
                return True
            return False

        except Exception as e:
            logger.error(f"Error dropping {name}: {e}", exc_info=True)
            self.print_error(f"Error dropping {name}: {e}")
            return False

    def create_multiple_objects(self, schemas: List[Dict]) -> tuple:
        """
        Create multiple database objects.

        Args:
            schemas: List of schema dictionaries

        Returns:
            Tuple of (success_count, failed_count)
        """
        start_time = time.time()
        success_count = 0
        failed_count = 0

        # Already sorted by order in load methods
        for schema in schemas:
            if self.create_object(schema):
                success_count += 1
            else:
                failed_count += 1

        elapsed_time = time.time() - start_time

        self.print_info(f"\n{'='*60}\n📊 Summary\n{'='*60}", Fore.CYAN)
        self.print_success(f"Created: {success_count} object(s)")
        if failed_count > 0:
            self.print_error(f"Failed: {failed_count} object(s)")
        self.print_info(f"⏱️  Total time: {elapsed_time:.2f} seconds", Fore.CYAN)

        logger.info(
            f"Batch operation completed: {success_count} succeeded, {failed_count} failed in {elapsed_time:.2f}s"
        )

        return success_count, failed_count

    def drop_all_objects(self, object_types: Optional[List[str]] = None) -> tuple:
        """
        Drop all objects of specified types.

        Args:
            object_types: List of types to drop (e.g., ['view', 'table'])
                         If None, drops views then tables

        Returns:
            Tuple of (success_count, failed_count)
        """
        start_time = time.time()

        if object_types is None:
            object_types = ["view", "matview", "table"]

        success_count = 0
        failed_count = 0

        all_schemas = self.get_all_schemas()

        if "view" in object_types and all_schemas["views"]:
            self.print_info("🗑️  Dropping all views...", Fore.RED)
            for view in all_schemas["views"]:
                if self.drop_object(view["name"], "VIEW"):
                    success_count += 1
                else:
                    failed_count += 1

        if "matview" in object_types and all_schemas["matviews"]:
            self.print_info("🗑️  Dropping all materialized views...", Fore.RED)
            for matview in all_schemas["matviews"]:
                if self.drop_object(matview["name"], "MATVIEW"):
                    success_count += 1
                else:
                    failed_count += 1

        if "table" in object_types and all_schemas["tables"]:
            self.print_info("🗑️  Dropping all tables...", Fore.RED)
            # Reverse order for dropping tables (dependencies)
            for table in reversed(all_schemas["tables"]):
                if self.drop_object(table["name"], "TABLE"):
                    success_count += 1
                else:
                    failed_count += 1

        elapsed_time = time.time() - start_time

        self.print_info(f"\n{'='*60}\n📊 Drop Summary\n{'='*60}", Fore.RED)
        self.print_success(f"Dropped: {success_count} object(s)")
        if failed_count > 0:
            self.print_error(f"Failed: {failed_count} object(s)")
        self.print_info(f"⏱️  Total time: {elapsed_time:.2f} seconds", Fore.RED)

        logger.info(
            f"Drop operation completed: {success_count} succeeded, {failed_count} failed in {elapsed_time:.2f}s"
        )

        return success_count, failed_count
