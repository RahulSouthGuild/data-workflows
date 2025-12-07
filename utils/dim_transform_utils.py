"""
Dimension Table Transformation Utilities

Provides schema validation, data type conversion, and table-specific ETL logic
for dimension tables like DimDealerMaster, DimCustomerMaster, DimHierarchy, etc.
"""

import logging
from typing import Optional, Dict, List, Any
import polars as pl

# Color codes for console output
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


def validate_schema_alignment(
    df: pl.DataFrame,
    schema_info: Dict[str, Any],
    table_name: str,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """Validate and align DataFrame columns with schema definition

    Args:
        df: Polars DataFrame to validate
        schema_info: Schema dictionary from schema file
        table_name: Name of the table
        logger: Optional logger instance

    Returns:
        Dictionary with validation results and aligned DataFrame
    """
    try:
        if logger:
            logger.info(f"🔍 Validating schema alignment for {table_name}...")

        # Get the schema entry for this table
        schema_entry = schema_info.get(table_name, {})

        # Extract columns dict (handle both formats)
        if "columns" in schema_entry:
            # Format: {table_name: {columns: {...}}}
            file_schema = schema_entry.get("columns", {})
        else:
            # Format: {table_name: {...}} where {...} directly contains column names
            file_schema = schema_entry

        schema_columns = set(file_schema.keys())
        df_columns = set(df.columns)

        # Create lowercase mapping for case-insensitive comparison
        df_columns_lower = {col.lower(): col for col in df_columns}
        schema_columns_lower = {col.lower(): col for col in schema_columns}

        # Find missing columns (case-insensitive)
        missing_in_df = set(schema_columns_lower.keys()) - set(df_columns_lower.keys())

        # Find extra columns (case-insensitive)
        extra_in_df = set(df_columns_lower.keys()) - set(schema_columns_lower.keys())

        # Get actual column names for extra columns to remove
        extra_columns_actual = [df_columns_lower[col] for col in extra_in_df]

        results = {
            "valid": True,
            "missing_columns": {schema_columns_lower[col] for col in missing_in_df},
            "extra_columns": set(extra_columns_actual),
            "dataframe": df,
        }

        # Add missing columns with NULL values
        if missing_in_df:
            missing_names = [schema_columns_lower[col] for col in missing_in_df]
            if logger:
                logger.warning(
                    f"{YELLOW}⚠️  Adding {len(missing_names)} missing columns: {missing_names}{RESET}"
                )
            df = df.with_columns([pl.lit(None).alias(col) for col in missing_names])

        # Remove extra columns
        if extra_columns_actual:
            if logger:
                logger.warning(
                    f"{YELLOW}⚠️  Removing {len(extra_columns_actual)} extra columns: {set(extra_columns_actual)}{RESET}"
                )
            df = df.drop(list(extra_columns_actual))

        results["dataframe"] = df

        if logger:
            logger.info(f"{GREEN}✓ Schema alignment complete - {len(df.columns)} columns{RESET}")

        return results

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Schema validation error: {e}{RESET}")
        raise


def apply_type_conversions(
    df: pl.DataFrame,
    schema_info: Dict[str, Any],
    table_name: str,
    logger: Optional[logging.Logger] = None,
) -> pl.DataFrame:
    """Apply data type conversions based on schema definition

    Args:
        df: Polars DataFrame
        schema_info: Schema dictionary from schema file
        table_name: Name of the table
        logger: Optional logger instance

    Returns:
        DataFrame with converted types
    """
    try:
        if logger:
            logger.info(f"🔄 Applying type conversions for {table_name}...")

        file_schema = schema_info.get(table_name, {})

        for column, info in file_schema.items():
            if column not in df.columns:
                continue

            new_name = info.get("name", column)
            data_type = info.get("data_type", "str")

            # Rename column if needed
            if column != new_name:
                df = df.rename({column: new_name})

            # Apply type conversions
            if data_type in ["int", "integer"]:
                df = df.with_columns(
                    pl.when(pl.col(new_name).is_null())
                    .then(None)
                    .otherwise(pl.col(new_name))
                    .cast(pl.Int64, strict=False)
                    .alias(new_name)
                )
            elif data_type in ["float", "double"]:
                df = df.with_columns(
                    pl.when(pl.col(new_name).is_null())
                    .then(None)
                    .otherwise(pl.col(new_name))
                    .cast(pl.Float64, strict=False)
                    .alias(new_name)
                )
            elif data_type in ["str", "string", "varchar"]:
                df = df.with_columns(pl.col(new_name).cast(pl.Utf8).alias(new_name))

        if logger:
            logger.info(f"{GREEN}✓ Type conversions complete{RESET}")

        return df

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Type conversion error: {e}{RESET}")
        raise


def apply_dim_dealer_etl(df: pl.DataFrame, logger: Optional[logging.Logger] = None) -> pl.DataFrame:
    """Apply ETL transformations specific to DimDealerMaster table

    Handles:
    - COALESCE logic for DealerGroupCode
    - CASE logic for DealerGroupName using active dealer mappings

    Args:
        df: Polars DataFrame for DimDealerMaster
        logger: Optional logger instance

    Returns:
        Transformed DataFrame
    """
    try:
        if logger:
            logger.info("🔄 Applying ETL enhancements for DimDealerMaster...")

        # 1. COALESCE logic for DealerGroupCode
        df = df.with_columns(
            [
                pl.coalesce([pl.col("DealerGroupCode"), pl.col("DealerCode")]).alias(
                    "DealerGroupCode"
                )
            ]
        )

        if logger:
            logger.info(f"{GREEN}  ✓ Applied COALESCE for DealerGroupCode{RESET}")

        # 2. Build dealer mapping from active dealers
        dealer_mapping = {}
        total_rows = df.height
        chunk_size = 50000

        if logger:
            logger.info(f"{CYAN}  📋 Creating dealer mapping from {total_rows:,} rows...{RESET}")

        for i in range(0, total_rows, chunk_size):
            chunk = df.slice(i, chunk_size)
            # Process chunk in batches to minimize memory usage
            for row in chunk.to_dicts():
                if row.get("ActiveFlag") == "True" and row.get("DealerCode"):
                    dealer_mapping[row["DealerCode"]] = row.get("DealerName")

            if logger:
                processed = min(i + chunk_size, total_rows)
                logger.info(f"{CYAN}     Processed {processed:,}/{total_rows:,}{RESET}")

        if logger:
            logger.info(
                f"{GREEN}  ✓ Created mapping with {len(dealer_mapping):,} active dealers{RESET}"
            )

        # 3. Apply the CASE logic for DealerGroupName
        def get_dealer_group_name(dealer_group_code, dealer_code, dealer_name):
            if dealer_group_code != dealer_code:
                return dealer_mapping.get(dealer_group_code, dealer_name)
            else:
                return dealer_name

        df = df.with_columns(
            [
                pl.struct(["DealerGroupCode", "DealerCode", "DealerName"])
                .map_elements(
                    lambda x: get_dealer_group_name(
                        x["DealerGroupCode"], x["DealerCode"], x["DealerName"]
                    ),
                    return_dtype=pl.Utf8,
                )
                .alias("DealerGroupName")
            ]
        )

        if logger:
            logger.info(f"{GREEN}✓ ETL enhancements complete for DimDealerMaster{RESET}")

        return df

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ DimDealerMaster ETL error: {e}{RESET}")
        raise


def apply_dim_customer_normalization(
    df: pl.DataFrame, logger: Optional[logging.Logger] = None
) -> pl.DataFrame:
    """Apply data normalization for DimCustomerMaster

    Normalizes TsiTerritoryName field (cleanup whitespace, uppercase conversion)

    Args:
        df: Polars DataFrame for DimCustomerMaster
        logger: Optional logger instance

    Returns:
        Normalized DataFrame
    """
    try:
        if logger:
            logger.info("🔄 Applying normalization for DimCustomerMaster (TsiTerritoryName)...")

        # Note: This operation is typically done at DB level via SQL
        # For Polars, we use regex_replace equivalent
        if "TsiTerritoryName" in df.columns:
            df = df.with_columns(
                pl.col("TsiTerritoryName")
                .str.to_uppercase()
                .str.strip()
                .str.replace_all(r"\s+", " ")
                .alias("TsiTerritoryName")
            )

            if logger:
                logger.info(f"{GREEN}✓ TsiTerritoryName normalization complete{RESET}")

        return df

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ DimCustomerMaster normalization error: {e}{RESET}")
        raise


def extract_email_hierarchy(row_emails: List[str]) -> str:
    """Extract and normalize email hierarchy for RLS mapping

    Converts email addresses to sanitized path format:
    example_at_company_dot_com -> example_at_company_dot_com

    Args:
        row_emails: List of email addresses from hierarchy columns

    Returns:
        Sanitized email path string (dot-separated)
    """
    emails = [email for email in row_emails if email]
    sanitized = []

    for email in emails:
        if not email:
            continue

        # Sanitize email
        sanitized_email = (
            email.strip()
            .lower()
            .replace("@", "_at_")
            .replace(".", "_dot_")
            .replace("-", "_")
            .replace(" ", "_")
        )

        # Keep only alphanumeric and underscore
        sanitized_email = "".join(c for c in sanitized_email if c.isalnum() or c == "_")
        sanitized.append(sanitized_email)

    # Remove consecutive duplicates and filter empty strings
    unique = [
        email
        for i, email in enumerate(sanitized)
        if email and (i == 0 or sanitized[i - 1] != email)
    ]

    # Join with dots to create path
    if len(unique) >= 2:
        return ".".join(unique)
    else:
        return None


def transform_dataframe(
    df: pl.DataFrame,
    schema_info: Dict[str, Any],
    table_name: str,
    logger: Optional[logging.Logger] = None,
) -> pl.DataFrame:
    """Main transformation orchestrator for dimension tables

    Applies schema validation, type conversions, and table-specific ETL logic

    Args:
        df: Polars DataFrame
        schema_info: Schema dictionary from schema file
        table_name: Name of the table
        logger: Optional logger instance

    Returns:
        Fully transformed DataFrame

    Raises:
        Exception: If any transformation step fails
    """
    try:
        initial_count = df.height
        if logger:
            logger.info(f"🔄 Transforming dataframe for {table_name}...")
            logger.info(f"   📈 Records before transformation: {initial_count:,}")

        # Step 1: Schema validation and alignment
        validation = validate_schema_alignment(df, schema_info, table_name, logger)
        df = validation["dataframe"]

        # Step 2: Type conversions
        df = apply_type_conversions(df, schema_info, table_name, logger)

        # Step 3: Table-specific ETL logic
        if table_name == "DimDealerMaster":
            df = apply_dim_dealer_etl(df, logger)

        if table_name == "DimCustomerMaster":
            df = apply_dim_customer_normalization(df, logger)

        final_count = df.height
        if logger:
            logger.info(f"   📊 Records after transformation: {final_count:,}")

            if initial_count != final_count:
                logger.warning(
                    f"{YELLOW}⚠️  Record count changed: "
                    f"{initial_count:,} -> {final_count:,}{RESET}"
                )
            else:
                logger.info(f"{GREEN}✓ Transformation complete - {final_count:,} records{RESET}")

        return df

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Transformation error for {table_name}: {e}{RESET}")
        raise
