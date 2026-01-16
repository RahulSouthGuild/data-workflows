"""
Unified Transformation Engine

Single source of truth for all data transformation and validation logic.
Used by ETL pipelines, parquet cleaner, and all data processing workflows.

Supports both legacy (single-tenant) and multi-tenant modes:
- Legacy: Uses db/schemas and db/column_mappings (backward compatible)
- Multi-tenant: Uses configs/tenants/{tenant_slug}/schemas and column_mappings

Core Functions:
- validate_and_transform_dataframe: Complete transformation with validation
- detect_data_overflows: Check for type mismatches and overflows
- apply_column_mappings: Map parquet columns to database columns
"""

import sys
import json
from pathlib import Path
from typing import Dict, Tuple, Optional, TYPE_CHECKING
from datetime import datetime

import polars as pl
from tqdm import tqdm

if TYPE_CHECKING:
    from orchestration.tenant_manager import TenantConfig

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.schema_validator import SchemaValidator  # noqa: E402

# Legacy paths (backward compatibility)
SCHEMAS_DIR = Path(__file__).parent.parent.parent / "db" / "schemas"
COLUMN_MAPPINGS_DIR = Path(__file__).parent.parent.parent / "db" / "column_mappings"
COMPUTED_COLUMNS_FILE = Path(__file__).parent.parent.parent / "db" / "computed_columns.json"

# Initialize legacy validator (backward compatibility)
validator = SchemaValidator.from_schema_files(SCHEMAS_DIR, COLUMN_MAPPINGS_DIR)

# Legacy computed columns config (backward compatibility)
def load_computed_columns_config(computed_columns_file: Path = None) -> Dict:
    """
    Load computed columns configuration from JSON file.

    Args:
        computed_columns_file: Optional path to computed columns file.
                              If None, uses legacy path.

    Returns:
        Dictionary of computed column configurations
    """
    file_path = computed_columns_file if computed_columns_file else COMPUTED_COLUMNS_FILE
    if file_path and file_path.exists():
        with open(file_path, "r") as f:
            return json.load(f)
    return {}


COMPUTED_COLUMNS_CONFIG = load_computed_columns_config()


def get_table_name_from_file(file_stem: str) -> str:
    """Map parquet filename to database table name."""
    # Comprehensive mapping for all PascalCase filenames to snake_case table names
    filename_to_table = {
        "DimCustomerMaster": "dim_customer_master",
        "DimDealerMaster": "dim_dealer_master",
        "DimHierarchy": "dim_hierarchy",
        "DimMaterial": "dim_material",
        "DimMaterialMapping": "dim_material_mapping",
        "DimSalesGroup": "dim_sales_group",
        "FactInvoiceDetails": "fact_invoice_details",
        "FactInvoiceSecondary": "fact_invoice_secondary",
        "RlsMaster": "rls_master",
    }

    # Check for exact match first
    if file_stem in filename_to_table:
        return filename_to_table[file_stem]

    # Check for partial matches (for files with suffixes like FactInvoiceSecondary_901)
    for key, value in filename_to_table.items():
        if file_stem.startswith(key):
            return value

    # Fallback: convert PascalCase to snake_case
    import re

    snake_case = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", file_stem)
    snake_case = re.sub("([a-z0-9])([A-Z])", r"\1_\2", snake_case)
    return snake_case.lower()


def generate_computed_columns(
    df: pl.DataFrame,
    table_name: str,
    tenant_config: Optional['TenantConfig'] = None,
    logger=None
) -> pl.DataFrame:
    """
    Generate computed columns for a dataframe before validation.

    Reads computed column definitions and applies them.
    Supports both legacy and multi-tenant modes.

    Args:
        df: Input Polars DataFrame
        table_name: Database table name (e.g., 'fact_invoice_secondary')
        tenant_config: Optional TenantConfig for multi-tenant mode
        logger: Optional logger instance

    Returns:
        DataFrame with computed columns added
    """
    # Determine which computed columns config to use
    if tenant_config is not None:
        # Multi-tenant mode: try tenant-specific first, fallback to shared
        computed_cols_config = load_computed_columns_config(tenant_config.computed_columns_path)

        # Fallback to shared config if tenant-specific doesn't have this table
        if table_name not in computed_cols_config:
            computed_cols_config = COMPUTED_COLUMNS_CONFIG
    else:
        # Legacy mode: use global config
        computed_cols_config = COMPUTED_COLUMNS_CONFIG

    if table_name not in computed_cols_config:
        return df

    def log(msg: str, level: str = "info"):
        if logger:
            getattr(logger, level)(msg)
        else:
            print(msg)

    # Map datatype strings to Polars types
    POLARS_TYPE_MAP = {
        "Utf8": pl.Utf8,
        "String": pl.Utf8,
        "VARCHAR": pl.Utf8,
        "Int32": pl.Int32,
        "Int64": pl.Int64,
        "INTEGER": pl.Int64,
        "Double": pl.Float64,
        "Float": pl.Float32,
        "DOUBLE": pl.Float64,
        "Boolean": pl.Boolean,
    }

    computed_cols = computed_cols_config[table_name]
    generated_cols = []
    failed_cols = []

    for col_name, col_config in computed_cols.items():
        col_type = col_config.get("type")
        polars_type = col_config.get("polars_type", "Utf8")
        target_dtype = POLARS_TYPE_MAP.get(polars_type, pl.Utf8)

        if col_type == "concatenation":
            # Handle concatenation of columns
            cols_to_concat = col_config.get("columns", [])
            separator = col_config.get("separator", "")

            # Check if all required columns exist
            missing_cols = [c for c in cols_to_concat if c not in df.columns]
            if missing_cols:
                failed_cols.append(f"{col_name} (missing: {', '.join(missing_cols)})")
                continue

            try:
                # Concatenate columns with separator, replacing nulls with "NULL" string
                # This ensures NULL values are represented as the string "NULL" instead of empty string
                concat_expr = pl.concat_str(
                    [pl.col(c).cast(pl.Utf8).fill_null("NULL") for c in cols_to_concat],
                    separator=separator,
                ).cast(target_dtype)
                df = df.with_columns(concat_expr.alias(col_name))
                generated_cols.append(col_name)
            except Exception as e:
                failed_cols.append(f"{col_name} ({str(e)})")

    # Log summary instead of individual columns
    if generated_cols:
        log(f"✓ Generated {len(generated_cols)} computed column(s): {', '.join(generated_cols)}", "info")
    if failed_cols:
        log(f"⚠️  Failed to generate {len(failed_cols)} computed column(s):", "warning")
        for failed in failed_cols:
            log(f"  - {failed}", "warning")

    return df


def validate_and_transform_dataframe(
    df: pl.DataFrame,
    table_name: str,
    tenant_config: Optional['TenantConfig'] = None,
    logger=None
) -> Tuple[pl.DataFrame, Dict]:
    """
    Transform and validate dataframe using column mappings.

    Core logic (UNCHANGED from parquet_cleaner.py):
    1. Load column mappings from JSON file
    2. Validate that mapped db_columns exist in the database schema
    3. Rename parquet columns to database column names
    4. Detect and handle data overflows
    5. Validate against database schema

    Args:
        df: Polars DataFrame from parquet file (with parquet column names)
        table_name: Database table name for schema lookup (e.g., 'dim_customer_master')
        logger: Optional logger instance for output

    Returns:
        Tuple of (transformed_dataframe, metadata_dict)
        - transformed_dataframe: Ready for database ingestion
        - metadata_dict: Contains transformation stats and warnings

    Raises:
        ValueError: If critical validation fails (type mismatch, numeric overflow)
    """

    def log(msg: str, level: str = "info"):
        """Unified logging function"""
        if logger:
            if level == "info":
                logger.info(msg)
            elif level == "warning":
                logger.warning(msg)
            elif level == "error":
                logger.error(msg)
        else:
            print(msg)

    metadata = {
        "table_name": table_name,
        "rows_before": len(df),
        "columns_before": len(df.columns),
        "invalid_mappings": [],
        "overflow_warnings": [],
        "transformation_errors": [],
    }

    # Determine paths and validator based on mode (tenant-aware or legacy)
    if tenant_config is not None:
        # Multi-tenant mode: try tenant-specific paths, fallback to shared
        tenant_schemas_dir = tenant_config.schema_path
        tenant_mappings_dir = tenant_config.column_mappings_path

        # Check if tenant-specific schemas exist (exclude __init__.py files)
        has_schema_files = False
        if tenant_schemas_dir.exists():
            schema_files = [f for f in tenant_schemas_dir.rglob("*.py") if f.name != "__init__.py"]
            has_schema_files = len(schema_files) > 0

        if has_schema_files:
            # Use tenant-specific schemas
            column_mappings_dir = tenant_mappings_dir
            schemas_dir = tenant_schemas_dir
            tenant_validator = SchemaValidator.from_schema_files(schemas_dir, column_mappings_dir)
            log(f"Using tenant-specific schemas from {schemas_dir}")
        else:
            # Fallback to shared schemas
            column_mappings_dir = COLUMN_MAPPINGS_DIR
            tenant_validator = validator  # Use global validator
            log(f"Tenant-specific schemas not found, using shared schemas from {SCHEMAS_DIR}")
    else:
        # Legacy mode: use global paths and validator
        column_mappings_dir = COLUMN_MAPPINGS_DIR
        tenant_validator = validator  # Use global validator


    log(f"🔄 Transforming data for table: {table_name}")

    # Step 1: Map table names to their JSON mapping files
    mapping_files = {
        "dim_customer_master": "02_DimCustomerMaster.json",
        "dim_dealer_master": "03_DimDealerMaster.json",
        "dim_hierarchy": "04_DimHierarchy.json",
        "dim_material": "06_DimMaterial.json",
        "dim_material_mapping": "01_DimMaterialMapping.json",
        "dim_sales_group": "05_DimSalesGroup.json",
        "fact_invoice_details": "07_FactInvoiceDetails.json",
        "fact_invoice_secondary": "08_FactInvoiceSecondary.json",
        "rls_master": "09_RlsMaster.json",
    }

    # Step 1.5: Get schema column names from validator
    schema_columns = tenant_validator.get_schema_columns(table_name)
    if not schema_columns:
        log(f"⚠️  Could not extract schema columns for {table_name}", "warning")
        schema_columns = set()

    # Step 2: Load and apply column mappings BEFORE computing columns
    # This is crucial: we need to rename columns (invoicedate → invoice_date) first
    # so that computed columns can reference the renamed snake_case column names
    json_filename = mapping_files.get(table_name)
    mapping_data = {}  # Initialize here so it's accessible in Step 4.3
    if json_filename:
        mapping_file = column_mappings_dir / json_filename
        try:
            with open(mapping_file) as f:
                mapping_data = json.load(f)

            # Step 3: Build rename dictionary and validate db_columns exist in schema
            rename_dict = {}
            invalid_mappings = []

            for parquet_col, col_info in mapping_data["columns"].items():
                db_col = col_info["db_column"]

                # VALIDATION: Check if db_column exists in schema
                if schema_columns and db_col.lower() not in schema_columns:
                    invalid_mappings.append(
                        {
                            "parquet_column": parquet_col,
                            "db_column": db_col,
                            "reason": "Column does not exist in database schema",
                        }
                    )

                if parquet_col != db_col:
                    rename_dict[parquet_col] = db_col

            # Step 3.5: Report invalid mappings with warnings/errors
            if invalid_mappings:
                log("❌ VALIDATION ERRORS: Invalid column mappings found", "error")
                for invalid in invalid_mappings[:10]:  # Show first 10
                    log(
                        f"  ✗ {invalid['parquet_column']:40} → {invalid['db_column']:40} ({invalid['reason']})",
                        "error",
                    )
                if len(invalid_mappings) > 10:
                    log(f"  ... and {len(invalid_mappings) - 10} more invalid mappings", "error")

                # Log all invalid mappings to file
                log_file = (
                    Path(__file__).parent.parent.parent
                    / "logs"
                    / f"invalid_mappings_{table_name}.log"
                )
                log_file.parent.mkdir(parents=True, exist_ok=True)
                with open(log_file, "w") as f:
                    f.write(f"Invalid Column Mappings for {table_name}\n")
                    f.write(f"Generated: {datetime.now().isoformat()}\n")
                    f.write(f"Total Invalid Mappings: {len(invalid_mappings)}\n\n")
                    f.write("Parquet Column → DB Column (Reason)\n")
                    f.write("-" * 100 + "\n")
                    for invalid in invalid_mappings:
                        f.write(
                            f"{invalid['parquet_column']:40} → {invalid['db_column']:40} ({invalid['reason']})\n"
                        )

                log(f"⚠️  Logged {len(invalid_mappings)} invalid mappings to {log_file}", "warning")
                metadata["invalid_mappings"] = invalid_mappings

            # Step 4: Apply column renaming
            if rename_dict:
                # Filter rename_dict to only include columns that exist in the dataframe
                existing_renames = {k: v for k, v in rename_dict.items() if k in df.columns}
                missing_columns = {k: v for k, v in rename_dict.items() if k not in df.columns}

                if missing_columns:
                    log(
                        f"⚠️  Skipping rename for {len(missing_columns)} missing columns (not in parquet)",
                        "warning",
                    )

                if existing_renames:
                    log(f"✓ Renaming {len(existing_renames)} columns using column mappings")
                    # Use tqdm for progress instead of logging each column
                    with tqdm(total=len(existing_renames), desc="Column renaming", unit="cols", leave=False, disable=logger is None) as pbar:
                        df = df.rename(existing_renames)
                        pbar.update(len(existing_renames))
                    log(f"✓ Column transformation complete ({len(existing_renames)} columns renamed)")
                else:
                    log(f"⚠️  No columns found to rename", "warning")
        except FileNotFoundError:
            log(f"⚠️  No mapping file found for {table_name}", "warning")
        except Exception as e:
            log(f"⚠️  Could not apply mappings for {table_name}: {e}", "warning")
            metadata["transformation_errors"].append(str(e))
    else:
        log(f"⚠️  No mapping file configured for {table_name}", "warning")

    # Step 4.3: APPLY TYPE CONVERSIONS FOR NUMERIC/INTEGER COLUMNS
    # Convert columns to proper data types based on mapping (handles string -> int conversion)
    if json_filename and mapping_file.exists():
        try:
            type_conversions = []
            conversion_errors = []
            columns_to_convert = mapping_data.get("columns", {})

            # Use tqdm for progress
            with tqdm(total=len(columns_to_convert), desc="Type conversions", unit="cols", leave=False, disable=logger is None) as pbar:
                for parquet_col, col_info in columns_to_convert.items():
                    db_col = col_info.get("db_column", parquet_col)
                    data_type = col_info.get("data_type", "").upper()

                    # Only convert columns that exist in the dataframe
                    if db_col in df.columns:
                        # Check if conversion is needed
                        current_type = str(df[db_col].dtype)

                        # Handle INTEGER/SMALLINT columns that are strings
                        if "INTEGER" in data_type or "SMALLINT" in data_type:
                            if "String" in current_type or "Utf8" in current_type:
                                try:
                                    df = df.with_columns(pl.col(db_col).cast(pl.Int32))
                                    type_conversions.append(f"{db_col} (String→Int32)")
                                except Exception as e:
                                    conversion_errors.append(f"{db_col}: {e}")

                        # Handle DOUBLE/FLOAT columns that are strings
                        elif "DOUBLE" in data_type or "FLOAT" in data_type:
                            if "String" in current_type or "Utf8" in current_type:
                                try:
                                    df = df.with_columns(pl.col(db_col).cast(pl.Float64))
                                    type_conversions.append(f"{db_col} (String→Float64)")
                                except Exception as e:
                                    conversion_errors.append(f"{db_col}: {e}")

                    pbar.update(1)

            if type_conversions:
                log(f"✓ Applied type conversions for {len(type_conversions)} columns")

            # Only log errors if there are any
            if conversion_errors:
                log(f"⚠️  Type conversion errors for {len(conversion_errors)} columns:", "warning")
                for error in conversion_errors[:3]:
                    log(f"  {error}", "warning")
                if len(conversion_errors) > 3:
                    log(f"  ... and {len(conversion_errors) - 3} more errors", "warning")
        except Exception as e:
            log(f"⚠️  Could not apply type conversions: {e}", "warning")

    # Step 4.4: APPLY TABLE-SPECIFIC FILTERS AFTER RENAME
    # FactInvoiceSecondary: Filter to recent invoices only (after 2023-03-31)
    if table_name == "fact_invoice_secondary" and "invoice_date" in df.columns:
        rows_before = len(df)
        try:
            # Cast to Int32 first (handle both string and numeric types)
            df = df.with_columns(pl.col("invoice_date").cast(pl.Int32))
            df = df.filter(pl.col("invoice_date") >= 20230401)
            rows_after = len(df)
            filtered_count = rows_before - rows_after
            if filtered_count > 0:
                log(
                    f"🔍 Filtered {filtered_count:,} old FactInvoiceSecondary records (before 2023-04-01)"
                )
            log(f"✓ Retained {rows_after:,} recent records")
        except Exception as e:
            log(f"⚠️  Could not apply date filter for FactInvoiceSecondary: {e}", "warning")

    # Step 4.5: GENERATE COMPUTED COLUMNS AFTER RENAME
    # Now that columns are renamed to snake_case (invoice_date, customer_code, etc),
    # we can safely generate computed columns that reference these renamed columns
    log(f"🔧 Generating computed columns for {table_name}...")
    df = generate_computed_columns(df, table_name, tenant_config, logger)

    # Step 5: DETECT OVERFLOWS AND TYPE MISMATCHES
    overflows = tenant_validator.detect_data_overflows(df, table_name)

    has_errors = False
    error_details = []

    # Check for type mismatches (STRICT - throw error)
    if overflows.get("type_mismatches"):
        has_errors = True
        log(f"❌ Type mismatch errors found ({len(overflows['type_mismatches'])} columns)", "error")
        for mismatch in overflows["type_mismatches"][:3]:
            error_msg = f"Column '{mismatch['column']}': Expected {mismatch['schema_type']}, got {mismatch['data_type']}"
            log(f"  ✗ {error_msg}", "error")
            error_details.append(error_msg)
        if len(overflows["type_mismatches"]) > 3:
            log(f"  ... and {len(overflows['type_mismatches']) - 3} more", "error")

    # Check for numeric overflow (STRICT - throw error)
    if overflows.get("numeric_overflows"):
        has_errors = True
        log(f"❌ Numeric overflow errors found ({len(overflows['numeric_overflows'])} columns)", "error")
        for overflow in overflows["numeric_overflows"][:3]:
            error_msg = f"Column '{overflow['column']}' ({overflow['schema_type']}): Data range [{overflow['min']}, {overflow['max']}] exceeds type range {overflow['range']}"
            log(f"  ✗ {error_msg}", "error")
            error_details.append(error_msg)
        if len(overflows["numeric_overflows"]) > 3:
            log(f"  ... and {len(overflows['numeric_overflows']) - 3} more", "error")

    # Throw error if type mismatches or numeric overflows detected
    if has_errors:
        error_msg = f"Data validation failed for {table_name}:\n" + "\n".join(error_details)
        log(f"{error_msg}", "error")
        raise ValueError(error_msg)

    # Check for VARCHAR overflow (AUTO-FIX with ALTER)
    if overflows.get("varchar_overflows"):
        log(f"⚠️  VARCHAR overflow detected ({len(overflows['varchar_overflows'])} columns)", "warning")
        for overflow in overflows["varchar_overflows"][:3]:
            col_name = overflow["column"]
            old_size = overflow["schema_size"]
            new_size = overflow["recommended_size"]
            log(f"  {col_name}: VARCHAR({old_size}) → VARCHAR({new_size})", "warning")
        if len(overflows["varchar_overflows"]) > 3:
            log(f"  ... and {len(overflows['varchar_overflows']) - 3} more", "warning")

            # Log overflow
            overflow_log = (
                Path(__file__).parent.parent.parent / "logs" / f"varchar_overflows_{table_name}.log"
            )
            overflow_log.parent.mkdir(parents=True, exist_ok=True)
            with open(overflow_log, "a") as f:
                f.write(
                    f"{datetime.now().isoformat()}: {col_name} VARCHAR({old_size}) → VARCHAR({new_size})\n"
                )

            metadata["overflow_warnings"].append(
                {
                    "column": col_name,
                    "old_size": old_size,
                    "new_size": new_size,
                }
            )

    # Step 5.5: Generate computed columns (BEFORE validation) - DUPLICATE, already done above
    # df = generate_computed_columns(df, table_name, tenant_config, logger)

    # Step 6: Validate against schema
    is_valid, error_msg, transformed_df = tenant_validator.validate_dataframe_against_schema(
        df, table_name
    )

    if not is_valid:
        error_msg = f"Validation Error for {table_name}: {error_msg}"
        log(f"{error_msg}", "error")
        raise ValueError(error_msg)

    log(f"✓ Validation passed ({len(transformed_df)} rows, {len(transformed_df.columns)} columns)")

    # Update metadata
    metadata["rows_after"] = len(transformed_df)
    metadata["columns_after"] = len(transformed_df.columns)
    metadata["success"] = True

    return transformed_df, metadata
