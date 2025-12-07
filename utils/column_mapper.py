"""
Column Mapping Utilities

Handles intelligent column name mapping between different naming conventions.
Uses schema_validator's normalization for flexible matching.

Key Features:
- Normalize column names (case, spaces, underscores, special chars)
- Match parquet columns to database schema columns
- Generate Stream Load column mapping headers
- Handle underscore/case conversion mismatches
"""

import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import polars as pl

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.logging_utils import get_pipeline_logger  # noqa: E402

logger = get_pipeline_logger(__name__)

# Color codes for logging
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


def normalize_column_name(col_name: str) -> str:
    """
    Normalize column names for flexible matching.

    Removes quotes, spaces, underscores, hyphens, special characters, and converts to lowercase.

    Examples:
    - invoicedate_df → invoicedatedf
    - InvoiceDateDF → invoicedatedf
    - customercode → customercode
    - customer_code → customercode
    - "Div" → div
    - branch code → branchcode

    Args:
        col_name: Original column name

    Returns:
        Normalized column name
    """
    # Remove quotes
    normalized = col_name.strip().strip('"').strip("'")
    # Remove spaces, underscores, hyphens, slashes, percent signs, and special chars
    normalized = re.sub(r"[\s_/%\-]", "", normalized)
    # Convert to lowercase
    normalized = normalized.lower()
    return normalized


def get_schema_lookup_key(col_name: str) -> str:
    """
    Generate schema lookup key for a column name.
    Similar to SchemaValidator._get_schema_lookup_key but exposed publicly.

    Args:
        col_name: Column name from parquet or schema

    Returns:
        Normalized lookup key
    """
    normalized = col_name.strip().strip('"').strip("'").lower()
    normalized = re.sub(r"[\s_/%\-]", "", normalized)
    return normalized


def find_column_mapping(
    parquet_col_name: str, schema_columns: Dict[str, str], parquet_data_type: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Find matching schema column for a parquet column using intelligent normalization.

    Strategy:
    1. Try exact match (same name)
    2. Try case-insensitive match
    3. Try normalized match (remove spaces, underscores, special chars)
    4. Find best type-compatible match

    Args:
        parquet_col_name: Name of column in parquet file
        schema_columns: Dict of {db_col_name: col_type} from schema
        parquet_data_type: Optional data type of parquet column for type compatibility scoring

    Returns:
        Tuple of (db_column_name, db_column_type) or (None, None) if no match found
    """
    # Step 1: Try exact match
    if parquet_col_name in schema_columns:
        return (parquet_col_name, schema_columns[parquet_col_name])

    # Step 2: Try case-insensitive match
    parquet_lower = parquet_col_name.lower()
    for db_col, col_type in schema_columns.items():
        if db_col.lower() == parquet_lower:
            return (db_col, col_type)

    # Step 3: Try normalized match (remove spaces, underscores, etc)
    parquet_normalized = normalize_column_name(parquet_col_name)

    candidates = []
    for db_col, col_type in schema_columns.items():
        db_normalized = normalize_column_name(db_col)

        if db_normalized == parquet_normalized:
            candidates.append((db_col, col_type))

    if candidates:
        # If only one match, return it
        if len(candidates) == 1:
            return candidates[0]

        # If multiple matches and we have parquet type info, score by type compatibility
        if parquet_data_type:
            best_match = None
            best_score = -1

            for db_col, col_type in candidates:
                score = _score_type_compatibility(parquet_data_type, col_type)
                if score > best_score:
                    best_score = score
                    best_match = (db_col, col_type)

            if best_match:
                return best_match

        # Return first candidate if no type scoring
        return candidates[0]

    # Step 4: Fuzzy match (find best partial match)
    best_score = 0
    best_match = None

    for db_col, col_type in schema_columns.items():
        score = _calculate_similarity_score(parquet_normalized, normalize_column_name(db_col))
        if score > best_score:
            best_score = score
            best_match = (db_col, col_type)

    # Only return if we found a good match (score > 0.7)
    if best_score > 0.7:
        return best_match

    return (None, None)


def _score_type_compatibility(parquet_type: str, db_type: str) -> int:
    """
    Score type compatibility between parquet and database types.
    Higher score = better match.

    Args:
        parquet_type: Polars data type as string (e.g., "String", "Int64")
        db_type: Database column type (e.g., "VARCHAR", "BIGINT")

    Returns:
        Compatibility score (higher is better)
    """
    score = 0

    # String types
    if "String" in parquet_type or "Utf8" in parquet_type:
        if "VARCHAR" in db_type.upper() or "TEXT" in db_type.upper():
            score = 100
        elif "INT" in db_type.upper():
            score = -50  # Wrong match
        else:
            score = 10

    # Integer types
    elif any(t in parquet_type for t in ["Int8", "Int16", "Int32", "Int64"]):
        if "INT" in db_type.upper():
            score = 100
        elif "VARCHAR" in db_type.upper():
            score = 10
        else:
            score = -50

    # Float types
    elif any(t in parquet_type for t in ["Float", "Double"]):
        if "FLOAT" in db_type.upper() or "DOUBLE" in db_type.upper():
            score = 100
        elif "INT" in db_type.upper():
            score = -50
        else:
            score = 10

    # Date/Time types
    elif "Date" in parquet_type or "Datetime" in parquet_type:
        if "DATE" in db_type.upper() or "DATETIME" in db_type.upper():
            score = 100
        else:
            score = 10

    return score


def _calculate_similarity_score(str1: str, str2: str) -> float:
    """
    Calculate similarity score between two strings (0.0 to 1.0).
    Uses basic matching heuristics.

    Args:
        str1: First string (normalized)
        str2: Second string (normalized)

    Returns:
        Score between 0.0 and 1.0
    """
    # If strings match exactly
    if str1 == str2:
        return 1.0

    # If one is substring of other
    if str1 in str2 or str2 in str1:
        shorter = min(len(str1), len(str2))
        longer = max(len(str1), len(str2))
        return shorter / longer

    # Count matching characters at same positions
    matching = sum(1 for a, b in zip(str1, str2) if a == b)
    total = max(len(str1), len(str2))

    if total == 0:
        return 0.0

    return matching / total


def build_column_mapping_header(
    parquet_columns: List[str],
    schema_columns: Dict[str, str],
    parquet_df: Optional[pl.DataFrame] = None,
) -> Tuple[str, Dict[str, str]]:
    """
    Build column mapping header for Stream Load and mapping dictionary.

    Creates mapping from parquet columns to database columns and generates
    the comma-separated column list for Stream Load header.

    Args:
        parquet_columns: List of column names in parquet file
        schema_columns: Dict of {db_col_name: col_type} from schema
        parquet_df: Optional parquet DataFrame for type inference

    Returns:
        Tuple of (column_mapping_header, mapping_dict)
        - column_mapping_header: Comma-separated list for Stream Load "columns" header
        - mapping_dict: {parquet_col: db_col} mapping for reference
    """
    mapping = {}
    mapped_columns = []
    unmapped = []

    for parquet_col in parquet_columns:
        # Get parquet data type if DataFrame provided
        parquet_type = None
        if parquet_df is not None and parquet_col in parquet_df.columns:
            parquet_type = str(parquet_df[parquet_col].dtype)

        # Find matching schema column
        db_col, db_type = find_column_mapping(parquet_col, schema_columns, parquet_type)

        if db_col:
            mapping[parquet_col] = db_col
            mapped_columns.append(db_col)
            logger.info(f"{GREEN}✓ Mapped {parquet_col} → {db_col} ({db_type}){RESET}")
        else:
            unmapped.append(parquet_col)
            logger.warning(f"{YELLOW}⚠ Could not map column: {parquet_col}{RESET}")

    if unmapped:
        logger.warning(f"{YELLOW}Unmapped columns: {', '.join(unmapped)}{RESET}")

    # Build Stream Load columns header
    column_mapping_header = ",".join(mapped_columns)

    return column_mapping_header, mapping


def validate_column_mapping(
    mapping: Dict[str, str], required_db_columns: Optional[List[str]] = None
) -> bool:
    """
    Validate that column mapping covers all required database columns.

    Args:
        mapping: {parquet_col: db_col} mapping dictionary
        required_db_columns: List of required database columns (if None, no validation)

    Returns:
        True if mapping is valid, False otherwise
    """
    if not mapping:
        logger.error(f"{RED}Column mapping is empty!{RESET}")
        return False

    if required_db_columns:
        mapped_db_cols = set(mapping.values())
        required_set = set(required_db_columns)

        missing = required_set - mapped_db_cols
        if missing:
            logger.error(f"{RED}Missing required columns: {', '.join(missing)}{RESET}")
            return False

    logger.info(f"{GREEN}✓ Column mapping validation passed ({len(mapping)} columns){RESET}")
    return True


def rename_dataframe_columns(df: pl.DataFrame, mapping: Dict[str, str]) -> pl.DataFrame:
    """
    Rename dataframe columns according to mapping.

    Args:
        df: Polars DataFrame with parquet columns
        mapping: {parquet_col: db_col} mapping dictionary

    Returns:
        DataFrame with renamed columns
    """
    rename_dict = {}
    for parquet_col in df.columns:
        if parquet_col in mapping:
            rename_dict[parquet_col] = mapping[parquet_col]

    if rename_dict:
        logger.info(f"{CYAN}Renaming {len(rename_dict)} columns...{RESET}")
        df = df.rename(rename_dict)
        logger.info(f"{GREEN}✓ Column rename complete{RESET}")

    return df


def extract_columns_from_schema(schema: Dict, table_name: Optional[str] = None) -> Dict[str, str]:
    """
    Extract column names and types from schema dictionary.

    Handles both formats:
    - {table_name: {columns: {col_name: col_type}}}
    - {columns: {col_name: col_type}}

    Args:
        schema: Schema dictionary
        table_name: Optional table name for nested schema format

    Returns:
        Dict of {column_name: column_type}
    """
    # Try nested format first
    if table_name and table_name in schema:
        schema_entry = schema[table_name]
        if isinstance(schema_entry, dict):
            if "columns" in schema_entry:
                return schema_entry["columns"]
            # Try assuming it's the columns dict directly
            return schema_entry

    # Try flat format
    if "columns" in schema:
        return schema["columns"]

    # Return schema as-is if it looks like columns dict
    if all(isinstance(v, str) for v in schema.values()):
        return schema

    logger.error(f"{RED}Could not extract columns from schema{RESET}")
    return {}


def debug_column_mapping(
    parquet_columns: List[str], schema_columns: Dict[str, str], verbose: bool = True
) -> Dict[str, List[str]]:
    """
    Debug column mapping to see all possible matches for each parquet column.

    Args:
        parquet_columns: List of parquet column names
        schema_columns: Dict of schema columns
        verbose: If True, print debug info

    Returns:
        Dict showing possible matches for each parquet column
    """
    debug_info = {}

    for parquet_col in parquet_columns:
        parquet_norm = normalize_column_name(parquet_col)

        possible_matches = []
        for schema_col in schema_columns.keys():
            schema_norm = normalize_column_name(schema_col)

            if parquet_norm == schema_norm:
                possible_matches.append(schema_col)

        debug_info[parquet_col] = possible_matches

        if verbose:
            if possible_matches:
                logger.info(f"{CYAN}{parquet_col}{RESET} → {possible_matches}")
            else:
                logger.warning(f"{YELLOW}{parquet_col}{RESET} → NO MATCH")

    return debug_info
