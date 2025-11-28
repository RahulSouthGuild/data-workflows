"""
Schema Validator Module
Validates parquet dataframes against database schemas defined in db/schemas
"""

import importlib.util
import sys
from pathlib import Path
from typing import Dict, Tuple
import polars as pl
import json
import re
from datetime import datetime

RED = "\033[91m"
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
RESET = "\033[0m"


class SchemaValidator:
    # Allowed type upgrades mapping (source_type: [allowed_target_types])
    ALLOWED_TYPE_UPGRADES = {
        # Floating point and decimal
        "FLOAT": ["DOUBLE", "DECIMAL"],
        "DOUBLE": ["DECIMAL"],
        "DECIMAL": ["DECIMAL"],  # allow bigger precision/scale
        # Integer widening
        "TINYINT": ["SMALLINT", "INT", "BIGINT", "LARGEINT", "FLOAT", "DOUBLE"],
        "SMALLINT": ["INT", "BIGINT", "LARGEINT", "FLOAT", "DOUBLE"],
        "INT": ["BIGINT", "LARGEINT", "FLOAT", "DOUBLE"],
        "BIGINT": ["LARGEINT", "DOUBLE"],
        # Date/time
        "DATE": ["DATETIME"],
    }

    @staticmethod
    def _is_allowed_type_upgrade(source_type: str, target_type: str) -> bool:
        """
        Check if upgrading from source_type to target_type is allowed.
        Handles only the explicitly allowed conversions.
        """

        # Normalize types (remove params, upper)
        def norm(t):
            t = t.upper()
            if "DECIMAL" in t:
                return "DECIMAL"
            if "VARCHAR" in t:
                return "VARCHAR"
            if "DATETIME" in t:
                return "DATETIME"
            if "DATE" in t:
                return "DATE"
            if "FLOAT" in t:
                return "FLOAT"
            if "DOUBLE" in t:
                return "DOUBLE"
            if "TINYINT" in t:
                return "TINYINT"
            if "SMALLINT" in t:
                return "SMALLINT"
            if "BIGINT" in t:
                return "BIGINT"
            if "LARGEINT" in t:
                return "LARGEINT"
            if "INT" in t:
                return "INT"
            return t

        s = norm(source_type)
        t = norm(target_type)
        allowed = SchemaValidator.ALLOWED_TYPE_UPGRADES.get(s, [])
        # Special: DECIMAL → DECIMAL always allowed if bigger
        if s == "DECIMAL" and t == "DECIMAL":
            return True
        return t in allowed

    """Validates dataframes against database schemas"""

    def __init__(self, tables: Dict[str, Dict]):
        """
        Initialize validator with table schemas

        Args:
            tables: Dictionary mapping table names to their schema definitions
        """
        self.tables = tables
        self.schema_changes = []  # Track all schema expansions
        self.logs_dir = Path(__file__).parent.parent / "logs"
        self.logs_dir.mkdir(exist_ok=True)

    @staticmethod
    def _normalize_column_name(col_name: str) -> str:
        """
        Normalize column names for flexible matching.
        Removes underscores, special characters, and converts to lowercase.
        Handles naming convention mismatches like:
          - invoicedate_df vs InvoiceDateDF
          - cgst% vs CGST_Percent
          - size/dimensions vs Size_Dimensions

        Args:
            col_name: Original column name

        Returns:
            Normalized column name
        """
        # Remove underscores, slashes, percent signs, and other special chars
        normalized = re.sub(r"[_/%]", "", col_name)
        # Convert to lowercase
        normalized = normalized.lower()
        return normalized

    def _log_schema_change(
        self, table_name: str, column_name: str, old_size: int, new_size: int, reason: str
    ):
        """Log schema changes to file and memory"""
        change_record = {
            "table": table_name,
            "column": column_name,
            "old_size": old_size,
            "new_size": new_size,
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        }
        self.schema_changes.append(change_record)

        # Log to file
        log_file = self.logs_dir / "schema_changes.log"
        try:
            with open(log_file, "a") as f:
                f.write(json.dumps(change_record) + "\n")
        except Exception as e:
            print(f"{RED}Error writing to schema change log: {e}{RESET}")

    def get_schema_change_summary(self) -> str:
        """Get summary of all schema changes"""
        if not self.schema_changes:
            return "No schema changes detected"

        summary = f"\n{YELLOW}{'='*70}\n"
        summary += f"SCHEMA CHANGES SUMMARY ({len(self.schema_changes)} changes)\n"
        summary += f"{'='*70}{RESET}\n"

        for change in self.schema_changes:
            summary += f"{YELLOW}Table: {change['table']}{RESET}\n"
            summary += f"  Column: {change['column']}\n"
            summary += f"  {change['old_size']} → {change['new_size']} (20% buffer applied)\n"
            summary += f"  Reason: {change['reason']}\n"
            summary += f"  Time: {change['timestamp']}\n\n"

        return summary

    @classmethod
    def from_schema_files(cls, schemas_dir: Path) -> "SchemaValidator":
        """
        Load schemas from db/schemas directory

        Args:
            schemas_dir: Path to schemas directory (e.g., db/schemas)

        Returns:
            SchemaValidator instance with loaded tables
        """
        tables = {}
        schemas_dir = Path(schemas_dir)

        # Get all .py files in schemas directory, sorted by name
        schema_files = sorted(schemas_dir.glob("*.py"))

        print(f"{CYAN}Loading schemas from {schemas_dir}{RESET}")

        for schema_file in schema_files:
            if schema_file.name.startswith("_"):
                continue

            try:
                # Dynamically import the schema file
                spec = importlib.util.spec_from_file_location(schema_file.stem, schema_file)
                module = importlib.util.module_from_spec(spec)
                sys.modules[schema_file.stem] = module
                spec.loader.exec_module(module)

                # Extract TABLE definition
                if hasattr(module, "TABLE"):
                    table_def = module.TABLE
                    table_name = table_def.get("name")
                    if table_name:
                        tables[table_name] = table_def
                        print(
                            f"{GREEN}✓ Loaded schema: {table_name} from {schema_file.name}{RESET}"
                        )
                    else:
                        print(
                            f"{RED}✗ No 'name' field in TABLE definition in {schema_file.name}{RESET}"
                        )
                else:
                    print(f"{RED}✗ No TABLE definition found in {schema_file.name}{RESET}")

            except Exception as e:
                print(f"{RED}✗ Error loading schema from {schema_file.name}: {str(e)}{RESET}")

        if not tables:
            raise ValueError(f"No schemas found in {schemas_dir}. Check the directory path.")

        print(f"{GREEN}Successfully loaded {len(tables)} table schemas{RESET}\n")
        return cls(tables)

    def _clean_numeric_strings(
        self, df: pl.DataFrame, col_name: str, expected_type: str
    ) -> pl.DataFrame:
        """
        Clean string data that should be numeric by converting to proper types.
        This is for data cleaning, not schema changes.

        Args:
            df: DataFrame to clean
            col_name: Column name to clean
            expected_type: Expected SQL type from schema

        Returns:
            DataFrame with cleaned column

        Raises:
            ValueError: If conversion fails with too many errors
        """
        try:
            original_nulls = df[col_name].is_null().sum()

            # Integer conversions
            if any(
                t in expected_type.upper()
                for t in ["TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT"]
            ):
                print(f"{CYAN}Converting {col_name} from string to integer (data cleaning){RESET}")

                # Convert string to integer, handling empty strings and nulls
                converted_df = df.with_columns(
                    [
                        pl.col(col_name)
                        .str.strip_chars()  # Remove whitespace
                        .replace("", None)  # Empty strings to null
                        .cast(pl.Int64, strict=False)  # Convert to int64
                        .alias(col_name)
                    ]
                )

                # Check conversion success rate
                new_nulls = converted_df[col_name].is_null().sum()
                failed_conversions = new_nulls - original_nulls

                if failed_conversions > 0:
                    failure_rate = failed_conversions / len(df)
                    if failure_rate > 0.1:  # More than 10% failures
                        raise ValueError(
                            f"Too many conversion failures for {col_name}: {failed_conversions} out of {len(df)} rows ({failure_rate:.1%})"
                        )
                    else:
                        print(
                            f"{YELLOW}Warning: {failed_conversions} values in {col_name} could not be converted to integer (set to null){RESET}"
                        )

                print(f"{GREEN}Successfully converted {col_name} to integer{RESET}")
                return converted_df

            # Float conversions
            elif any(t in expected_type.upper() for t in ["FLOAT", "DOUBLE", "DECIMAL"]):
                print(f"{CYAN}Converting {col_name} from string to float (data cleaning){RESET}")

                converted_df = df.with_columns(
                    [
                        pl.col(col_name)
                        .str.strip_chars()  # Remove whitespace
                        .replace("", None)  # Empty strings to null
                        .cast(pl.Float64, strict=False)  # Convert to float64
                        .alias(col_name)
                    ]
                )

                # Check conversion success rate
                new_nulls = converted_df[col_name].is_null().sum()
                failed_conversions = new_nulls - original_nulls

                if failed_conversions > 0:
                    failure_rate = failed_conversions / len(df)
                    if failure_rate > 0.1:  # More than 10% failures
                        raise ValueError(
                            f"Too many conversion failures for {col_name}: {failed_conversions} out of {len(df)} rows ({failure_rate:.1%})"
                        )
                    else:
                        print(
                            f"{YELLOW}Warning: {failed_conversions} values in {col_name} could not be converted to float (set to null){RESET}"
                        )

                print(f"{GREEN}Successfully converted {col_name} to float{RESET}")
                return converted_df

            # Date/DateTime conversions
            elif "DATE" in expected_type.upper():
                print(f"{CYAN}Converting {col_name} from string to date (data cleaning){RESET}")

                # Try multiple date formats
                date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d", "%Y-%m-%d %H:%M:%S"]

                for fmt in date_formats:
                    try:
                        if "DATETIME" in expected_type.upper():
                            converted_df = df.with_columns(
                                [
                                    pl.col(col_name)
                                    .str.strptime(pl.Datetime, format=fmt, strict=False)
                                    .alias(col_name)
                                ]
                            )
                        else:
                            converted_df = df.with_columns(
                                [
                                    pl.col(col_name)
                                    .str.strptime(pl.Date, format=fmt, strict=False)
                                    .alias(col_name)
                                ]
                            )

                        new_nulls = converted_df[col_name].is_null().sum()
                        failed_conversions = new_nulls - original_nulls

                        if failed_conversions <= len(df) * 0.1:  # Allow 10% conversion failures
                            if failed_conversions > 0:
                                print(
                                    f"{YELLOW}Warning: {failed_conversions} values in {col_name} could not be converted to date (set to null){RESET}"
                                )
                            print(
                                f"{GREEN}Successfully converted {col_name} to date using format {fmt}{RESET}"
                            )
                            return converted_df
                    except Exception:
                        continue

                # If all formats failed
                raise ValueError(f"Could not convert {col_name} to date - no suitable format found")

        except Exception as e:
            error_msg = f"Data cleaning failed for column {col_name}: {str(e)}"
            print(f"{RED}Error: {error_msg}{RESET}")
            raise ValueError(error_msg)

        return df

    def _attempt_type_conversion(
        self, df: pl.DataFrame, col_name: str, expected_type: str
    ) -> pl.DataFrame:
        """
        Attempt to convert column to expected type if possible.

        Args:
            df: DataFrame to modify
            col_name: Column name to convert
            expected_type: Expected SQL type from schema

        Returns:
            DataFrame with converted column if successful, original if not
        """
        try:
            # Only attempt conversion if current data is string
            if df[col_name].dtype != pl.Utf8:
                return df

            print(f"{CYAN}Attempting to convert {col_name} from string to {expected_type}{RESET}")

            # Integer conversions
            if any(
                t in expected_type.upper()
                for t in ["TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT"]
            ):
                # Try to convert to integer, handling nulls and invalid values
                converted_df = df.with_columns(
                    [
                        pl.col(col_name)
                        .str.strip_chars()
                        .replace("", None)
                        .cast(pl.Int64, strict=False)
                        .alias(col_name)
                    ]
                )

                # Check if conversion was successful (no nulls introduced where there weren't any)
                original_nulls = df[col_name].is_null().sum()
                new_nulls = converted_df[col_name].is_null().sum()

                if new_nulls > original_nulls:
                    print(
                        f"{YELLOW}Warning: Converting {col_name} to integer introduced {new_nulls - original_nulls} nulls. Keeping as string.{RESET}"
                    )
                    return df
                else:
                    print(f"{GREEN}Successfully converted {col_name} to integer{RESET}")
                    return converted_df

            # Float conversions
            elif any(t in expected_type.upper() for t in ["FLOAT", "DOUBLE", "DECIMAL"]):
                converted_df = df.with_columns(
                    [
                        pl.col(col_name)
                        .str.strip_chars()
                        .replace("", None)
                        .cast(pl.Float64, strict=False)
                        .alias(col_name)
                    ]
                )

                original_nulls = df[col_name].is_null().sum()
                new_nulls = converted_df[col_name].is_null().sum()

                if new_nulls > original_nulls:
                    print(
                        f"{YELLOW}Warning: Converting {col_name} to float introduced {new_nulls - original_nulls} nulls. Keeping as string.{RESET}"
                    )
                    return df
                else:
                    print(f"{GREEN}Successfully converted {col_name} to float{RESET}")
                    return converted_df

            # Date/DateTime conversions
            elif "DATE" in expected_type.upper():
                # Try multiple date formats
                date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"]

                for fmt in date_formats:
                    try:
                        converted_df = df.with_columns(
                            [
                                pl.col(col_name)
                                .str.strptime(pl.Date, format=fmt, strict=False)
                                .alias(col_name)
                            ]
                        )

                        original_nulls = df[col_name].is_null().sum()
                        new_nulls = converted_df[col_name].is_null().sum()

                        if new_nulls <= original_nulls + (
                            len(df) * 0.1
                        ):  # Allow 10% conversion failures
                            print(
                                f"{GREEN}Successfully converted {col_name} to date using format {fmt}{RESET}"
                            )
                            return converted_df
                    except Exception:
                        continue

                print(
                    f"{YELLOW}Warning: Could not convert {col_name} to date. Keeping as string.{RESET}"
                )

        except Exception as e:
            print(
                f"{YELLOW}Warning: Type conversion failed for {col_name}: {e}. Keeping original type.{RESET}"
            )

        return df

    def validate_dataframe_against_schema(
        self, df: pl.DataFrame, table_name: str
    ) -> Tuple[bool, str, pl.DataFrame]:
        """
        Validate dataframe against table schema.

        This validates that:
        1. All columns in the dataframe are defined in the schema (case-insensitive)
        2. Column data types and constraints match the schema

        It does NOT require the dataframe to have all schema columns,
        as parquet files may contain only a subset of columns.

        Args:
            df: Polars DataFrame to validate
            table_name: Name of the table schema to validate against

        Returns:
            Tuple of (is_valid, error_message)
        """
        if table_name not in self.tables:
            return False, f"Table '{table_name}' not found in loaded schemas", df

        table_def = self.tables[table_name]

        # Get column definitions from CREATE TABLE statement
        schema_str = table_def.get("schema", "")
        expected_columns = self._extract_columns_from_schema(schema_str)

        if not expected_columns:
            return (True, "Could not extract column definitions, skipping validation", df)

        # Normalize column names: remove underscores + special chars + lowercase for flexible matching
        # This handles naming convention mismatches like invoicedate_df vs InvoiceDateDF
        # Also handles special chars like cgst% vs CGST_Percent or size/dimensions vs Size_Dimensions
        expected_columns_normalized = {
            self._normalize_column_name(k): (k, v) for k, v in expected_columns.items()
        }
        df_columns = set(df.columns)

        # Check if all dataframe columns are defined in the schema (normalized comparison)
        extra_columns = []
        for col_name in df_columns:
            if self._normalize_column_name(col_name) not in expected_columns_normalized:
                extra_columns.append(col_name)

        if extra_columns:
            print(f"{RED}Warning: Dataframe has columns not in schema: {set(extra_columns)}{RESET}")
            # Don't fail validation for extra columns, just warn

        # Validate columns that exist in both dataframe and schema
        for col_name in df_columns:
            col_normalized = self._normalize_column_name(col_name)
            if col_normalized not in expected_columns_normalized:
                continue

            schema_col, col_type = expected_columns_normalized[col_normalized]

            # Attempt data type conversion for data cleaning (string numbers to actual numbers)
            if df[col_name].dtype == pl.Utf8 and not "VARCHAR" in col_type.upper():
                df = self._clean_numeric_strings(df, col_name, col_type)

            try:
                # VARCHAR logic (as before)
                if "VARCHAR" in col_type:
                    varchar_limit = self._extract_varchar_limit(col_type)
                    if varchar_limit and df[col_name].dtype == pl.Utf8:
                        try:
                            max_len = df[col_name].str.lengths().max()
                        except AttributeError:
                            max_len = (
                                df[col_name]
                                .map_elements(lambda x: len(x) if x else 0, return_dtype=pl.UInt32)
                                .max()
                            )
                        if max_len is not None and max_len > varchar_limit:
                            new_size = int(max_len * 1.2)
                            new_size = min(new_size, 5000)
                            self._log_schema_change(
                                table_name,
                                col_name,
                                varchar_limit,
                                new_size,
                                f"Data contains {max_len} chars, expanded with 20% buffer",
                            )
                            print(
                                f"{YELLOW}⚠️  SCHEMA EXPANSION: {col_name}{RESET}\n"
                                f"  Found data: {max_len} chars\n"
                                f"  Schema limit: {varchar_limit}\n"
                                f"  New limit: {new_size} (with 20% buffer)\n"
                                f"  Logged to: {self.logs_dir / 'schema_changes.log'}{RESET}"
                            )
                # Numeric and date type upgrades
                else:
                    # Map Polars dtype to SQL type
                    pl_dtype = str(df[col_name].dtype).upper()

                    # Skip validation if column contains string data but schema expects numeric
                    if df[col_name].dtype == pl.Utf8:
                        # Check if schema expects numeric type but data is string
                        if any(
                            t in col_type.upper()
                            for t in [
                                "TINYINT",
                                "SMALLINT",
                                "INT",
                                "BIGINT",
                                "LARGEINT",
                                "FLOAT",
                                "DOUBLE",
                                "DECIMAL",
                            ]
                        ):
                            # This should not happen anymore since we clean the data above
                            print(
                                f"{YELLOW}Warning: Column {col_name} still contains string data after cleaning attempt. Schema expects {col_type}.{RESET}"
                            )
                            continue

                    # Integer types - only validate if data is actually numeric
                    if (
                        any(
                            t in col_type.upper()
                            for t in ["TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT"]
                        )
                        and df[col_name].dtype != pl.Utf8
                    ):
                        # Get min/max for the column
                        try:
                            min_val = df[col_name].min()
                            max_val = df[col_name].max()

                            # Skip if min_val or max_val is None (all nulls)
                            if min_val is None or max_val is None:
                                continue

                        except Exception as e:
                            print(
                                f"{YELLOW}Warning: Could not get min/max for {col_name}: {e}{RESET}"
                            )
                            continue

                        # Define StarRocks type ranges
                        type_ranges = {
                            "TINYINT": (-128, 127),
                            "SMALLINT": (-32768, 32767),
                            "INT": (-2147483648, 2147483647),
                            "BIGINT": (-9223372036854775808, 9223372036854775807),
                            "LARGEINT": (None, None),  # practically unlimited
                        }

                        # Find current type
                        current_type = None
                        for t in ["TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT"]:
                            if t in col_type.upper():
                                current_type = t
                                break

                        # Find next allowed type if overflow
                        if current_type and current_type != "LARGEINT":
                            rng = type_ranges[current_type]
                            if (rng[0] is not None and min_val < rng[0]) or (
                                rng[1] is not None and max_val > rng[1]
                            ):
                                # Find next allowed type
                                allowed = self.ALLOWED_TYPE_UPGRADES.get(current_type, [])
                                for next_type in allowed:
                                    if next_type in type_ranges:
                                        next_range = type_ranges[next_type]
                                        if (next_range[0] is None or min_val >= next_range[0]) and (
                                            next_range[1] is None or max_val <= next_range[1]
                                        ):
                                            # Log the schema change
                                            self._log_schema_change(
                                                table_name,
                                                col_name,
                                                current_type,
                                                next_type,
                                                f"Data out of range for {current_type}: min={min_val}, max={max_val}",
                                            )
                                            print(
                                                f"{YELLOW}⚠️  SCHEMA TYPE UPGRADE: {col_name}{RESET}\n  Found data: min={min_val}, max={max_val}\n  Schema type: {current_type}\n  New type: {next_type}\n  Logged to: {self.logs_dir / 'schema_changes.log'}{RESET}"
                                            )
                                            break
                    # FLOAT/DOUBLE/DECIMAL upgrades - only validate if data is actually numeric
                    elif (
                        any(t in col_type.upper() for t in ["FLOAT", "DOUBLE", "DECIMAL"])
                        and df[col_name].dtype != pl.Utf8
                    ):
                        # For FLOAT, check if values require DOUBLE/DECIMAL
                        if "FLOAT" in col_type.upper():
                            # If any value is not representable as float32, suggest upgrade
                            # (Polars uses float64 by default, so just check dtype)
                            if pl_dtype == "FLOAT64":
                                # Prefer DOUBLE, then DECIMAL
                                self._log_schema_change(
                                    table_name,
                                    col_name,
                                    "FLOAT",
                                    "DOUBLE",
                                    f"Data requires DOUBLE precision (float64 detected)",
                                )
                                print(
                                    f"{YELLOW}⚠️  SCHEMA TYPE UPGRADE: {col_name}{RESET}\n  Data requires DOUBLE precision (float64 detected)\n  Schema type: FLOAT\n  New type: DOUBLE\n  Logged to: {self.logs_dir / 'schema_changes.log'}{RESET}"
                                )
                        # For DOUBLE, check if DECIMAL is needed (not auto-detectable, placeholder)
                        # For DECIMAL, check if precision/scale needs to be increased (not auto-detectable, placeholder)
                        # (Advanced: user can add logic for precision/scale checks)
                    # DATE → DATETIME - only validate if data is actually date/datetime type
                    elif (
                        "DATE" in col_type.upper()
                        and "DATETIME" not in col_type.upper()
                        and df[col_name].dtype != pl.Utf8
                    ):
                        # If any value has time component, suggest DATETIME
                        # (Polars reads as date/datetime, so check dtype)
                        if pl_dtype in [
                            "DATETIME[NS]",
                            "DATETIME[US]",
                            "DATETIME[MS]",
                            "DATETIME[S]",
                        ]:
                            self._log_schema_change(
                                table_name,
                                col_name,
                                "DATE",
                                "DATETIME",
                                f"Data contains time component, upgrade to DATETIME",
                            )
                            print(
                                f"{YELLOW}⚠️  SCHEMA TYPE UPGRADE: {col_name}{RESET}\n  Data contains time component\n  Schema type: DATE\n  New type: DATETIME\n  Logged to: {self.logs_dir / 'schema_changes.log'}{RESET}"
                            )
            except Exception as e:
                print(f"{RED}Warning: Could not validate column {col_name}: {str(e)}{RESET}")

        return True, "Validation passed", df

    @staticmethod
    def _extract_columns_from_schema(schema_str: str) -> Dict[str, str]:
        """
        Extract column names and types from CREATE TABLE statement

        Args:
            schema_str: SQL CREATE TABLE statement

        Returns:
            Dictionary mapping column names to their types
        """
        columns = {}
        try:
            # Find content between parentheses
            start = schema_str.find("(")
            end = schema_str.rfind(")")

            if start == -1 or end == -1:
                return columns

            content = schema_str[start + 1 : end]

            # Split by commas
            column_defs = content.split(",")

            for col_def in column_defs:
                col_def = col_def.strip()

                # Skip empty lines
                if not col_def:
                    continue

                # Stop at DISTRIBUTED or PROPERTIES keywords
                if col_def.upper().startswith("DISTRIBUTED") or col_def.upper().startswith(
                    "PROPERTIES"
                ):
                    break

                # Extract column name and type
                # Split by whitespace to get column name and type
                parts = col_def.split()
                if len(parts) >= 2:
                    col_name = parts[0]
                    col_type = " ".join(parts[1:])

                    # Skip if this looks like a keyword instead of a column
                    if col_name.upper() not in ["DISTRIBUTED", "PROPERTIES", "BUCKETS"]:
                        columns[col_name] = col_type

        except Exception as e:
            print(f"{RED}Error extracting columns from schema: {str(e)}{RESET}")

        return columns

    @staticmethod
    def _extract_varchar_limit(col_type: str) -> int:
        """
        Extract VARCHAR limit from type string

        Args:
            col_type: Column type string (e.g., "VARCHAR(118)")

        Returns:
            Integer limit or 0 if not found
        """
        try:
            if "VARCHAR" in col_type:
                start = col_type.find("(")
                end = col_type.find(")")
                if start != -1 and end != -1:
                    return int(col_type[start + 1 : end])
        except Exception:
            pass
        return 0

    def get_alter_table_statements(self) -> list:
        """
        Generate ALTER TABLE statements for all detected schema changes (VARCHAR and type upgrades).

        Returns:
            List of SQL ALTER TABLE statements
        """
        if not self.schema_changes:
            return []

        alter_statements = []
        for change in self.schema_changes:
            table_name = change["table"]
            column_name = change["column"]
            old_size = change.get("old_size")
            new_size = change.get("new_size")
            # For type upgrades, old_size/new_size are type names
            # For VARCHAR, they are int sizes
            if isinstance(new_size, int):
                # VARCHAR expansion
                sql = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} VARCHAR({new_size});"
            else:
                # Type upgrade (e.g., INT → BIGINT, FLOAT → DOUBLE, DATE → DATETIME, etc)
                sql = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} {new_size};"
            alter_statements.append(
                {
                    "table": table_name,
                    "column": column_name,
                    "sql": sql,
                    "timestamp": change["timestamp"],
                }
            )

        return alter_statements

    def print_alter_statements(self):
        """Print all ALTER TABLE statements to console for manual review"""
        alter_statements = self.get_alter_table_statements()

        if not alter_statements:
            print(f"{GREEN}✓ No schema changes require ALTER TABLE statements{RESET}")
            return

        print(f"\n{YELLOW}{'='*80}{RESET}")
        print(f"{YELLOW}ALTER TABLE STATEMENTS (Review before executing){RESET}")
        print(f"{YELLOW}{'='*80}{RESET}")

        for i, stmt in enumerate(alter_statements, 1):
            print(f"\n{i}. {CYAN}Table: {stmt['table']}, Column: {stmt['column']}{RESET}")
            print(f"   {stmt['sql']}")

        print(f"\n{YELLOW}{'='*80}{RESET}")
        print(f"{GREEN}Total statements to review: {len(alter_statements)}{RESET}")
        print(f"All statements saved to: {self.logs_dir / 'alter_statements.sql'}")
        print(f"{YELLOW}{'='*80}{RESET}\n")

    def save_alter_statements_to_file(self) -> bool:
        """
        Save all ALTER TABLE statements to a SQL file.

        Returns:
            True if successful, False otherwise
        """
        alter_statements = self.get_alter_table_statements()

        if not alter_statements:
            return False

        try:
            sql_file = self.logs_dir / "alter_statements.sql"
            with open(sql_file, "w") as f:
                f.write("-- Auto-generated ALTER TABLE statements from schema validation\n")
                f.write(f"-- Generated: {datetime.now().isoformat()}\n")
                f.write("-- Review carefully before executing!\n\n")

                for stmt in alter_statements:
                    f.write(f"-- Table: {stmt['table']}, Column: {stmt['column']}\n")
                    f.write(f"-- Change detected at: {stmt['timestamp']}\n")
                    f.write(f"{stmt['sql']}\n\n")

            print(f"{GREEN}✓ ALTER statements saved to: {sql_file}{RESET}")
            return True
        except Exception as e:
            print(f"{RED}Error saving ALTER statements: {e}{RESET}")
            return False
