#!/usr/bin/env python3
"""
Tenant-Aware Seed Data Loader for StarRocks

This script loads permanent CSV seed data (dim_sales_group, dim_material_mapping, etc.)
into tenant-specific databases.

Features:
- Interactive tenant selection menu
- Loads seed data from tenant-specific configs/tenants/{tenant}/seeds/
- Uses tenant-specific database connections
- Column mapping support for CSV → database column transformation
- Batch INSERT for performance
- Truncate option before loading

Usage:
    python db/load_seed_data_tenant.py              # Interactive menu
    python db/load_seed_data_tenant.py --tenant pidilite --load-all
    python db/load_seed_data_tenant.py --tenant pidilite --load dim_sales_group

Author: DataWiz Team
"""

import sys
import logging
import csv
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import pymysql
from colorama import init, Fore, Style

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from orchestration.tenant_manager import TenantManager, TenantConfig  # noqa: E402

# Initialize colorama
init(autoreset=True)

# Configure logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "seed_data_load_tenant.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


class TenantSeedDataLoader:
    """Load seed data from CSV files into tenant-specific StarRocks databases"""

    def __init__(self, tenant_config: TenantConfig):
        """
        Initialize loader with tenant configuration

        Args:
            tenant_config: TenantConfig object with database and path information
        """
        self.tenant_config = tenant_config
        self.tenant_id = tenant_config.tenant_slug
        self.connection = None
        self.seeds_dir = tenant_config.seeds_path
        self.column_mappings = self._load_column_mappings()
        self.seed_config = self._load_seed_config()

        logger.info(f"[{self.tenant_id}] TenantSeedDataLoader initialized")
        logger.info(f"[{self.tenant_id}] Seeds directory: {self.seeds_dir}")
        logger.info(f"[{self.tenant_id}] Configured seeds: {list(self.seed_config.keys())}")

    def print_info(self, msg: str, color=Fore.CYAN):
        """Print colored info message with tenant context"""
        print(f"{color}[{self.tenant_id}]{Style.RESET_ALL} {msg}")

    def print_success(self, msg: str):
        """Print colored success message"""
        print(f"{Fore.GREEN}[{self.tenant_id}] ✅{Style.RESET_ALL} {msg}")

    def print_warning(self, msg: str):
        """Print colored warning message"""
        print(f"{Fore.YELLOW}[{self.tenant_id}] ⚠️ {Style.RESET_ALL} {msg}")

    def print_error(self, msg: str):
        """Print colored error message"""
        print(f"{Fore.RED}[{self.tenant_id}] ❌{Style.RESET_ALL} {msg}")

    def _load_seed_config(self) -> Dict:
        """
        Load SEED_MAPPING.py from tenant seeds directory

        Returns:
            Dictionary mapping table_name → seed configuration
        """
        seed_mapping_file = self.seeds_dir / "SEED_MAPPING.py"

        if not seed_mapping_file.exists():
            logger.warning(f"[{self.tenant_id}] SEED_MAPPING.py not found: {seed_mapping_file}")
            return {}

        try:
            # Dynamically import SEED_CONFIG from tenant's SEED_MAPPING.py
            import importlib.util

            spec = importlib.util.spec_from_file_location("seed_mapping", seed_mapping_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            seed_config = getattr(module, "SEED_CONFIG", {})
            logger.info(f"[{self.tenant_id}] Loaded {len(seed_config)} seed configurations")
            return seed_config

        except Exception as e:
            logger.error(f"[{self.tenant_id}] Failed to load SEED_MAPPING.py: {e}")
            return {}

    def _load_column_mappings(self) -> Dict[str, Dict]:
        """
        Load column mappings from tenant's column_mappings/ YAML files

        Returns:
            Dictionary mapping table_name → {csv_key → {db_column, data_type, ...}}
        """
        import yaml

        mappings_dir = self.tenant_config.column_mappings_path
        column_mappings = {}

        if not mappings_dir.exists():
            logger.warning(f"[{self.tenant_id}] Column mappings directory not found: {mappings_dir}")
            return column_mappings

        try:
            for mapping_file in sorted(mappings_dir.glob("*.yaml")):
                try:
                    with open(mapping_file, "r", encoding="utf-8") as f:
                        mapping_data = yaml.safe_load(f)
                        table_name = mapping_data.get("table_name")
                        if table_name and "columns" in mapping_data:
                            column_mappings[table_name] = mapping_data["columns"]
                            logger.debug(
                                f"[{self.tenant_id}] Loaded mapping for {table_name}: {len(mapping_data['columns'])} columns"
                            )
                except Exception as e:
                    logger.error(f"[{self.tenant_id}] Failed to load mapping from {mapping_file}: {e}")

            logger.info(f"[{self.tenant_id}] Total table mappings loaded: {len(column_mappings)}")
            return column_mappings

        except Exception as e:
            logger.error(f"[{self.tenant_id}] Error loading column mappings: {e}")
            return column_mappings

    def _get_db_column_from_csv_header(self, csv_header: str, table_name: str) -> str:
        """
        Map CSV column header to database column name using column_mappings

        Args:
            csv_header: Column name from CSV file (PascalCase or mixed case)
            table_name: Target table name to look up in mappings

        Returns:
            Database column name (snake_case) or original csv_header if not found
        """
        if table_name not in self.column_mappings:
            return csv_header

        table_mappings = self.column_mappings[table_name]
        normalized_header = csv_header.lower()

        if normalized_header in table_mappings:
            db_col = table_mappings[normalized_header].get("db_column", csv_header)
            logger.debug(f"[{self.tenant_id}] Mapped {csv_header} → {db_col}")
            return db_col

        return csv_header

    def connect(self) -> bool:
        """Connect to tenant-specific StarRocks database"""
        try:
            self.print_info(f"🔌 Connecting to {self.tenant_config.database_name}...", Fore.CYAN)
            logger.info(
                f"[{self.tenant_id}] Connecting to {self.tenant_config.database_host}:{self.tenant_config.database_port}"
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

            self.print_success(
                f"Connected to '{self.tenant_config.database_name}' as '{self.tenant_config.database_user}'"
            )
            logger.info(f"[{self.tenant_id}] Connected to StarRocks successfully")
            return True

        except Exception as e:
            self.print_error(f"Failed to connect: {e}")
            logger.error(f"[{self.tenant_id}] Connection failed: {e}", exc_info=True)
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info(f"[{self.tenant_id}] Disconnected from StarRocks")

    def read_csv(self, csv_file: str) -> Tuple[List[str], List[Dict]]:
        """
        Read CSV file from tenant's seeds directory

        Args:
            csv_file: Filename in tenant's seeds/ directory

        Returns:
            Tuple of (headers list, rows list of dicts)
        """
        csv_path = self.seeds_dir / csv_file

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        logger.info(f"[{self.tenant_id}] Reading CSV: {csv_path}")
        headers = []
        rows = []

        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames or []
                for row in reader:
                    # Filter empty values
                    cleaned_row = {k: v for k, v in row.items() if v is not None and v.strip()}
                    rows.append(cleaned_row)

            logger.info(f"[{self.tenant_id}] Read {len(rows)} rows from {csv_file}")
            self.print_info(f"📄 Read {len(rows):,} rows from {csv_file}")
            return headers, rows

        except Exception as e:
            logger.error(f"[{self.tenant_id}] CSV read error: {e}")
            raise

    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate table before loading

        Args:
            table_name: Name of table to truncate

        Returns:
            True if successful, False otherwise
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            cursor.close()

            self.print_info(f"🗑️  Truncated {table_name}")
            logger.info(f"[{self.tenant_id}] Truncated table: {table_name}")
            return True

        except Exception as e:
            self.print_warning(f"Failed to truncate {table_name}: {e}")
            logger.warning(f"[{self.tenant_id}] Truncate failed for {table_name}: {e}")
            return False

    def load_data_batch(
        self, table_name: str, headers: List[str], rows: List[Dict], batch_size: int = 10000
    ) -> int:
        """
        Load data using batch INSERT statements with column mapping

        Args:
            table_name: Target table name
            headers: Column names from CSV
            rows: Row data as list of dicts
            batch_size: Rows per INSERT batch (default: 1000)

        Returns:
            Number of rows inserted
        """
        if not rows:
            self.print_warning(f"No data to load into {table_name}")
            return 0

        try:
            cursor = self.connection.cursor()
            total_inserted = 0

            # Map CSV headers to database column names
            db_columns = []
            mapping_count = 0
            for csv_header in headers:
                db_col = self._get_db_column_from_csv_header(csv_header, table_name)
                db_columns.append(db_col)
                if db_col != csv_header:
                    mapping_count += 1

            if mapping_count > 0:
                self.print_info(f"  📍 Mapped {mapping_count} columns", Fore.YELLOW)

            # Process in batches
            total_batches = (len(rows) + batch_size - 1) // batch_size

            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                values_list = []

                for row in batch:
                    # Create values in column order (using CSV headers as keys)
                    values = tuple(row.get(h, None) for h in headers)
                    formatted_values = []

                    for val in values:
                        if val is None or val == "":
                            formatted_values.append("NULL")
                        else:
                            # Escape quotes
                            escaped_val = str(val).replace("'", "''")
                            formatted_values.append(f"'{escaped_val}'")

                    values_list.append(f"({', '.join(formatted_values)})")

                # Build INSERT statement using mapped database column names
                columns = ", ".join(db_columns)
                values_str = ", ".join(values_list)
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES {values_str};"

                try:
                    cursor.execute(insert_sql)
                    batch_count = len(batch)
                    total_inserted += batch_count

                    batch_num = (i // batch_size) + 1
                    progress = f"[{batch_num}/{total_batches}]"
                    self.print_info(f"✓ Batch {progress}: {batch_count:,} rows inserted")
                    logger.info(f"[{self.tenant_id}] Batch {batch_num} inserted: {batch_count} rows")

                except Exception as e:
                    batch_num = (i // batch_size) + 1
                    self.print_error(f"Batch {batch_num} failed: {str(e)[:80]}")
                    logger.error(f"[{self.tenant_id}] Batch {batch_num} error: {e}")
                    # Continue with next batch

            cursor.close()
            return total_inserted

        except Exception as e:
            self.print_error(f"Load data error: {e}")
            logger.error(f"[{self.tenant_id}] Load data error: {e}", exc_info=True)
            return 0

    def load_seed(self, table_name: str, truncate: bool = False) -> int:
        """
        Load seed data for a specific table

        Args:
            table_name: Table name from SEED_CONFIG
            truncate: Whether to truncate before loading

        Returns:
            Number of rows loaded
        """
        # Get seed config
        config = self.seed_config.get(table_name)
        if not config:
            self.print_error(f"Table '{table_name}' not in SEED_CONFIG")
            logger.error(f"[{self.tenant_id}] Unknown table: {table_name}")
            return 0

        if not config.get("enabled", False):
            self.print_warning(f"Table '{table_name}' is disabled in SEED_CONFIG")
            logger.warning(f"[{self.tenant_id}] Table disabled: {table_name}")
            return 0

        csv_file = config.get("csv_file")
        description = config.get("description", "")

        self.print_info(f"\n📥 Loading {table_name}", Fore.CYAN)
        if description:
            self.print_info(f"   {description}")

        try:
            # Read CSV
            headers, rows = self.read_csv(csv_file)

            # Truncate if requested
            if truncate:
                self.truncate_table(table_name)
            elif config.get("truncate_before_load", False):
                self.truncate_table(table_name)

            # Load data
            rows_loaded = self.load_data_batch(table_name, headers, rows)

            if rows_loaded > 0:
                self.print_success(f"Loaded {rows_loaded:,} rows into {table_name}")
                logger.info(f"[{self.tenant_id}] Successfully loaded {rows_loaded} rows into {table_name}")
            else:
                self.print_warning(f"No rows loaded into {table_name}")

            return rows_loaded

        except Exception as e:
            self.print_error(f"Failed to load {table_name}: {e}")
            logger.error(f"[{self.tenant_id}] Load failed for {table_name}: {e}", exc_info=True)
            return 0

    def load_all(self, truncate: bool = False) -> int:
        """
        Load all enabled seeds

        Args:
            truncate: Whether to truncate tables first

        Returns:
            Total rows loaded
        """
        total_loaded = 0
        enabled_seeds = {k: v for k, v in self.seed_config.items() if v.get("enabled", False)}

        if not enabled_seeds:
            self.print_warning("No enabled seeds found in SEED_CONFIG")
            return 0

        self.print_info(f"\n🌱 Loading {len(enabled_seeds)} seed table(s)...\n", Fore.CYAN)

        for table_name in enabled_seeds.keys():
            rows = self.load_seed(table_name, truncate=truncate)
            total_loaded += rows

        print("\n" + "=" * 70)
        self.print_success("Seed data loading complete!")
        self.print_info(f"Total rows loaded: {total_loaded:,}")
        print("=" * 70 + "\n")

        return total_loaded

    def get_row_count(self, table_name: str) -> int:
        """Get current row count in table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"[{self.tenant_id}] Failed to get row count for {table_name}: {e}")
            return 0

    def show_status(self):
        """Show current row counts for all seed tables"""
        print("\n" + "=" * 70)
        print(f"{Fore.CYAN}[{self.tenant_id}] 📊 Seed Data Status{Style.RESET_ALL}")
        print("=" * 70)

        if not self.seed_config:
            print(f"{Fore.YELLOW}No seed tables configured{Style.RESET_ALL}")
            print("=" * 70 + "\n")
            return

        for table_name, config in self.seed_config.items():
            count = self.get_row_count(table_name)
            enabled = "✅" if config.get("enabled") else "❌"
            description = config.get("description", "")

            print(f"\n{Fore.YELLOW}{enabled} {table_name}{Style.RESET_ALL}")
            print(f"   Rows: {count:>10,}")
            if description:
                print(f"   Info: {description}")

        print("\n" + "=" * 70 + "\n")


def select_tenant(tenant_manager: TenantManager) -> Optional[TenantConfig]:
    """
    Display tenant selection menu and return selected tenant config

    Args:
        tenant_manager: TenantManager instance

    Returns:
        Selected TenantConfig or None if user cancelled
    """
    tenants = tenant_manager.get_all_enabled_tenants()

    if not tenants:
        print(f"{Fore.RED}❌ No enabled tenants found in tenant_registry.yaml{Style.RESET_ALL}")
        return None

    print("\n" + "=" * 70)
    print(f"{Fore.CYAN}🌱 Tenant Seed Data Loader - Select Tenant{Style.RESET_ALL}")
    print("=" * 70)

    for idx, tenant_config in enumerate(tenants, 1):
        db_name = tenant_config.database_name
        tenant_name = tenant_config.tenant_name
        print(f"  {idx}. {Fore.YELLOW}{tenant_name}{Style.RESET_ALL} ({db_name})")

    print(f"  0. Exit")
    print()

    while True:
        try:
            choice = input(f"{Fore.YELLOW}Select tenant [1-{len(tenants)}]: {Style.RESET_ALL}").strip()

            if choice == "0":
                return None

            idx = int(choice) - 1
            if 0 <= idx < len(tenants):
                return tenants[idx]
            else:
                print(f"{Fore.RED}Invalid choice. Please enter a number between 1 and {len(tenants)}.{Style.RESET_ALL}")

        except ValueError:
            print(f"{Fore.RED}Invalid input. Please enter a number.{Style.RESET_ALL}")
        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Operation cancelled{Style.RESET_ALL}")
            return None


def show_operations_menu(tenant_config: TenantConfig, loader: TenantSeedDataLoader):
    """
    Show operations menu for selected tenant

    Args:
        tenant_config: Selected tenant configuration
        loader: TenantSeedDataLoader instance
    """
    print("\n" + "=" * 70)
    print(f"{Fore.CYAN}[{tenant_config.tenant_slug}] Seed Data Operations{Style.RESET_ALL}")
    print("=" * 70)

    seed_tables = list(loader.seed_config.keys())

    if seed_tables:
        print(f"\n{Fore.YELLOW}Load Individual Seed Tables:{Style.RESET_ALL}")
        for idx, table_name in enumerate(seed_tables, 1):
            config = loader.seed_config[table_name]
            enabled = "✅" if config.get("enabled") else "❌"
            print(f"  {idx}. {enabled} {table_name} (Truncate + Load)")

    print(f"\n{Fore.YELLOW}Quick Actions:{Style.RESET_ALL}")
    print(f"  {len(seed_tables) + 1}. Load All Enabled Seeds (Truncate + Load)")
    print(f"  {len(seed_tables) + 2}. Show Seed Data Status")
    print(f"  0. Back to Tenant Selection")
    print()


def handle_tenant_operations(tenant_config: TenantConfig):
    """
    Handle all operations for selected tenant

    Args:
        tenant_config: Selected tenant configuration
    """
    loader = TenantSeedDataLoader(tenant_config)

    if not loader.connect():
        print(f"{Fore.RED}Failed to connect to database. Returning to tenant selection.{Style.RESET_ALL}")
        return

    try:
        while True:
            show_operations_menu(tenant_config, loader)
            seed_tables = list(loader.seed_config.keys())
            choice = input(f"{Fore.YELLOW}Enter choice: {Style.RESET_ALL}").strip()

            if choice == "0":
                break

            # Load all enabled seeds
            elif choice == str(len(seed_tables) + 1):
                loader.load_all(truncate=True)

            # Show status
            elif choice == str(len(seed_tables) + 2):
                loader.show_status()

            # Load specific seed
            else:
                try:
                    idx = int(choice) - 1
                    if 0 <= idx < len(seed_tables):
                        table_name = seed_tables[idx]
                        loader.load_seed(table_name, truncate=True)
                    else:
                        print(f"{Fore.YELLOW}⚠️  Invalid choice. Please try again.{Style.RESET_ALL}")
                except ValueError:
                    print(f"{Fore.YELLOW}⚠️  Invalid input. Please enter a number.{Style.RESET_ALL}")

    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Operation cancelled{Style.RESET_ALL}")
    finally:
        loader.disconnect()


def main():
    """Main entry point"""
    # Initialize tenant manager
    tenant_manager = TenantManager(PROJECT_ROOT / "configs")

    # Parse command line arguments
    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()

        if cmd == "--help":
            print(__doc__)
            return 0

        # Check for --tenant flag
        tenant_slug = None
        if cmd == "--tenant" and len(sys.argv) > 2:
            tenant_slug = sys.argv[2]
            # Get next command if exists
            if len(sys.argv) > 3:
                cmd = sys.argv[3].lower()
            else:
                print(f"{Fore.RED}Missing command after --tenant{Style.RESET_ALL}")
                print("Usage: --tenant TENANT_SLUG --load-all | --load TABLE_NAME | --status")
                return 1
        else:
            print(f"{Fore.RED}Missing --tenant flag{Style.RESET_ALL}")
            print("Usage: --tenant TENANT_SLUG --load-all | --load TABLE_NAME | --status")
            return 1

        # Get tenant config
        tenant_config = tenant_manager.get_tenant_by_slug(tenant_slug)
        if not tenant_config:
            print(f"{Fore.RED}Tenant '{tenant_slug}' not found or not enabled{Style.RESET_ALL}")
            return 1

        loader = TenantSeedDataLoader(tenant_config)

        # Execute command
        if cmd == "--load-all":
            if not loader.connect():
                return 1
            try:
                loader.load_all(truncate=True)
                return 0
            finally:
                loader.disconnect()

        elif cmd == "--load" and len(sys.argv) > 4:
            table_name = sys.argv[4]
            if not loader.connect():
                return 1
            try:
                loader.load_seed(table_name, truncate=True)
                return 0
            finally:
                loader.disconnect()

        elif cmd == "--status":
            if not loader.connect():
                return 1
            try:
                loader.show_status()
                return 0
            finally:
                loader.disconnect()

        else:
            print(f"{Fore.RED}Unknown command: {cmd}{Style.RESET_ALL}")
            print("Usage: --tenant TENANT_SLUG --load-all | --load TABLE_NAME | --status")
            return 1
    else:
        # Interactive mode
        try:
            while True:
                tenant_config = select_tenant(tenant_manager)
                if tenant_config is None:
                    print(f"\n{Fore.CYAN}👋 Goodbye!{Style.RESET_ALL}")
                    break

                handle_tenant_operations(tenant_config)

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Operation cancelled{Style.RESET_ALL}")

        return 0


if __name__ == "__main__":
    sys.exit(main())
