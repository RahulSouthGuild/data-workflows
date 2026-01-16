"""
Script to update starrocks_stream_loader.py with tenant-aware changes.

This script applies changes to make StarRocksStreamLoader tenant-aware
while maintaining backward compatibility.
"""

from pathlib import Path
import re

# Read the file
file_path = Path("core/loaders/starrocks_stream_loader.py")
with open(file_path, "r") as f:
    content = f.read()

# Track changes
changes_made = []

# Change 1: Add TYPE_CHECKING import
if "from typing import" in content and "TYPE_CHECKING" not in content:
    content = content.replace(
        "from typing import Dict, List, Tuple, Optional",
        "from typing import Dict, List, Tuple, Optional, TYPE_CHECKING"
    )
    changes_made.append("Added TYPE_CHECKING import")

# Change 2: Add TenantConfig type hint
if "TYPE_CHECKING" in content and "from orchestration.tenant_manager import TenantConfig" not in content:
    # Find the imports section and add the conditional import
    import_section = """from tqdm import tqdm


"""
    type_checking_section = """from tqdm import tqdm

if TYPE_CHECKING:
    from orchestration.tenant_manager import TenantConfig


"""
    content = content.replace(import_section, type_checking_section)
    changes_made.append("Added TYPE_CHECKING conditional import for TenantConfig")

# Change 3: Update __init__ method signature and docstring
old_init_pattern = r'''    def __init__\(
        self, config: Dict, logger=None, debug: bool = False, max_error_ratio: float = 0\.0
    \):
        """
        Initialize Stream Loader with StarRocks configuration\.

        Args:
            config: StarRocks config dict with:
                - host: StarRocks host
                - port: MySQL port \(3306\)
                - http_port: HTTP port \(8040\)
                - user: Username
                - password: Password
                - database: Database name
            logger: Optional logger instance for logging output
            debug: Enable debug logging \(default: False\)
            max_error_ratio: Maximum error ratio \(0\.0 = strict/no errors, 1\.0 = 100% tolerance\)
                           \(default: 0\.0 for production safety\)
        """'''

new_init = '''    def __init__(
        self,
        config: Dict = None,
        tenant_config: Optional['TenantConfig'] = None,
        logger=None,
        debug: bool = False,
        max_error_ratio: float = 0.0
    ):
        """
        Initialize Stream Loader with StarRocks configuration.

        Supports both legacy (config dict) and multi-tenant (tenant_config) modes.

        Args:
            config: Optional StarRocks config dict with:
                - host: StarRocks host
                - port: MySQL port (9030)
                - http_port: HTTP port (8040)
                - user: Username
                - password: Password
                - database: Database name
            tenant_config: Optional TenantConfig for multi-tenant mode
            logger: Optional logger instance for logging output
            debug: Enable debug logging (default: False)
            max_error_ratio: Maximum error ratio (0.0 = strict/no errors, 1.0 = 100% tolerance)
                           (default: 0.0 for production safety)

        Note: Either config or tenant_config must be provided.
        """
        # Extract config from tenant_config if provided
        if tenant_config is not None:
            self.config = {
                'host': tenant_config.database_host,
                'port': tenant_config.database_port,
                'http_port': tenant_config.database_http_port,
                'user': tenant_config.database_user,
                'password': tenant_config.database_password,
                'database': tenant_config.database_name,
            }
            self.tenant_slug = tenant_config.tenant_slug
        elif config is not None:
            self.config = config
            self.tenant_slug = None  # Legacy mode
        else:
            raise ValueError("Either config or tenant_config must be provided")'''

content = re.sub(old_init_pattern, new_init, content, flags=re.DOTALL)
changes_made.append("Updated __init__ method with tenant_config support")

# Change 4: Update __init__ method body to remove self.config assignment
# Remove the old "self.config = config" line since we now set it conditionally
old_assignment = "        self.logger = logger"
new_assignment = """        # self.config is now set above based on tenant_config or config parameter
        self.logger = logger"""

content = content.replace(old_assignment, new_assignment)
changes_made.append("Updated config assignment logic in __init__")

# Change 5: Add tenant context to logging
# Update _log method to include tenant slug
old_log_method = '''    def _log(self, msg: str, level: str = "info"):
        """Log message to logger or stdout."""
        if self.logger:
            log_fn = getattr(self.logger, level, None)
            if log_fn:
                log_fn(msg)
        elif self.debug:
            print(msg)'''

new_log_method = '''    def _log(self, msg: str, level: str = "info"):
        """Log message to logger or stdout with tenant context."""
        # Add tenant prefix if in multi-tenant mode
        if self.tenant_slug:
            msg = f"[{self.tenant_slug}] {msg}"

        if self.logger:
            log_fn = getattr(self.logger, level, None)
            if log_fn:
                log_fn(msg)
        elif self.debug:
            print(msg)'''

content = content.replace(old_log_method, new_log_method)
changes_made.append("Updated _log method to include tenant context")

# Write the updated file
with open(file_path, "w") as f:
    f.write(content)

# Print summary
print("=" * 80)
print("STARROCKS STREAM LOADER UPDATE COMPLETED")
print("=" * 80)
print(f"\nChanges made: {len(changes_made)}")
for change in changes_made:
    print(f"  ✓ {change}")

print("\n✅ File updated successfully!")
print(f"📄 Updated file: {file_path.absolute()}")
