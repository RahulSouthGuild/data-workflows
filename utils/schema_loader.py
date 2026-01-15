"""
Schema Loader Utility
Handles loading column mappings from individual JSON files in db/column_mappings/.
Maps parquet filenames to schema files and provides schema validation.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple


def get_column_mappings_dir() -> Path:
    """Get the path to the column_mappings directory."""
    # Try multiple locations
    base_paths = [
        Path("/home/rahul/RahulSouthGuild/datawiz/db/column_mappings"),
        Path.cwd() / "db" / "column_mappings",
        Path(__file__).parent.parent / "db" / "column_mappings",
    ]

    for path in base_paths:
        if path.exists() and path.is_dir():
            return path

    return Path.cwd() / "db" / "column_mappings"


def load_column_mapping(mapping_file: Path) -> Optional[Dict]:
    """
    Load a column mapping JSON file.

    Args:
        mapping_file: Path to the JSON mapping file

    Returns:
        Parsed JSON dict or None if file not found/invalid
    """
    try:
        with open(mapping_file, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON from {mapping_file}: {e}")
        return None


def find_mapping_file_for_table(
    table_name: str, mappings_dir: Optional[Path] = None
) -> Optional[Path]:
    """
    Find the column mapping file for a given table name.

    Args:
        table_name: Table name (e.g., 'DimMaterial', 'DimCustomerMaster')
        mappings_dir: Column mappings directory (auto-detected if None)

    Returns:
        Path to the mapping file or None if not found
    """
    if mappings_dir is None:
        mappings_dir = get_column_mappings_dir()

    # Search for files that match the table name
    for mapping_file in sorted(mappings_dir.glob("*.json")):
        if mapping_file.name == "INTEGRATION_STATUS.md" or mapping_file.name == "README.md":
            continue

        mapping_data = load_column_mapping(mapping_file)
        if mapping_data and mapping_data.get("table_name"):
            # Compare table names (case-insensitive, handle snake_case vs PascalCase)
            schema_table_name = mapping_data["table_name"].lower()
            search_name = table_name.lower().replace(" ", "_")

            if schema_table_name == search_name or search_name.replace(
                "_", ""
            ) == schema_table_name.replace("_", ""):
                return mapping_file

    return None


def get_schema_from_mapping(mapping_file: Path) -> Optional[Dict]:
    """
    Extract schema information from a column mapping file.

    Args:
        mapping_file: Path to the mapping file

    Returns:
        Schema dict with columns info or None
    """
    mapping_data = load_column_mapping(mapping_file)
    if not mapping_data:
        return None

    table_name = mapping_data.get("table_name")
    if not table_name:
        return None

    # Return in the format expected by transform_dataframe: {table_name: {columns, ...}}
    schema = {
        table_name: {
            "description": mapping_data.get("description", ""),
            "columns": mapping_data.get("columns", {}),
            "table_name": table_name,
        }
    }

    return schema


def get_schema_for_table(table_name: str, mappings_dir: Optional[Path] = None) -> Optional[Dict]:
    """
    Load schema for a specific table by name.

    Args:
        table_name: Table name to find schema for
        mappings_dir: Column mappings directory (auto-detected if None)

    Returns:
        Schema dict or None if not found
    """
    mapping_file = find_mapping_file_for_table(table_name, mappings_dir)
    if not mapping_file:
        return None

    return get_schema_from_mapping(mapping_file)


def get_all_schemas(mappings_dir: Optional[Path] = None) -> Dict[str, Dict]:
    """
    Load all available schemas from the column_mappings directory.

    Args:
        mappings_dir: Column mappings directory (auto-detected if None)

    Returns:
        Dict mapping table names to their schemas
    """
    if mappings_dir is None:
        mappings_dir = get_column_mappings_dir()

    all_schemas = {}

    for mapping_file in sorted(mappings_dir.glob("*.json")):
        if not mapping_file.suffix == ".json":
            continue
        if mapping_file.name in ["INTEGRATION_STATUS.md", "README.md"]:
            continue

        mapping_data = load_column_mapping(mapping_file)
        if mapping_data and "table_name" in mapping_data:
            table_name = mapping_data["table_name"]
            all_schemas[table_name] = {
                "file": mapping_file.name,
                "description": mapping_data.get("description", ""),
                "columns": mapping_data.get("columns", {}),
            }

    return all_schemas


def blob_folder_to_table_name(blob_folder_name: str) -> Optional[str]:
    """
    Map a blob folder name to the actual table name using DIMENSION_TABLES mapping.

    Examples:
        'DimHierarchy' -> 'DimHierarchy'
        'DimDealer_MS' -> 'DimDealerMaster'
        'DimCustomerMaster' -> 'DimCustomerMaster'

    Args:
        blob_folder_name: The folder name from Azure blob (e.g., 'DimDealer_MS')

    Returns:
        The actual table name or None if not found
    """
    try:
        from utils.pipeline_config import DIMENSION_TABLES

        # DIMENSION_TABLES maps table_name -> blob_path
        # We need to reverse it: blob_folder -> table_name
        for table_name, blob_path in DIMENSION_TABLES.items():
            # Extract folder name from blob path (e.g., "Incremental/DimDealer_MS/LatestData/" -> "DimDealer_MS")
            path_parts = blob_path.strip("/").split("/")
            if path_parts[0] == "Incremental" and len(path_parts) > 1:
                folder = path_parts[1]
                if folder == blob_folder_name or folder.lower() == blob_folder_name.lower():
                    return table_name

        return None
    except ImportError:
        return None


def parquet_filename_to_table_name(parquet_filename: str) -> str:
    """
    Convert a parquet filename to a table name.

    Examples:
        'DimMaterial.parquet' -> 'DimMaterial'
        'Dim_Material.parquet' -> 'DimMaterial'
        'dim_material.parquet' -> 'DimMaterial'
        'DimHierarchy_9999_2025_12_02_14:01:46.parquet' -> 'DimHierarchy'
        'DimDealer_MS_9999_2025_12_02_14:01:37.parquet' -> 'DimDealerMaster'

    Args:
        parquet_filename: Parquet filename (with or without extension)

    Returns:
        Table name matching actual database table
    """
    # Remove extension
    name = parquet_filename.replace(".parquet", "").replace(".PARQUET", "")

    # Extract table name: get everything before timestamp pattern or sequence number
    # Pattern: TableName[_suffix]_NNNN_YYYY_MM_DD_HH:MM:SS
    # We want: TableName[_suffix] part only

    # Split by underscores and check for date pattern (YYYY or numeric sequence)
    parts = name.split("_")
    table_parts = []

    for part in parts:
        # Stop if we hit a date pattern (4 digit year or sequence number like 9999)
        if part.isdigit() and len(part) in (4, 5):  # 4-digit year or 5-digit sequence
            break
        table_parts.append(part)

    # If we didn't find any parts, use the whole name
    if not table_parts:
        table_parts = parts[:1]

    # Join back with underscores
    blob_folder_name = "_".join(table_parts)

    # Try to map blob folder name to actual table name using DIMENSION_TABLES
    mapped_table = blob_folder_to_table_name(blob_folder_name)
    if mapped_table:
        return mapped_table

    # Fallback: Convert snake_case to PascalCase
    if "_" in blob_folder_name:
        pascal_parts = blob_folder_name.split("_")
        table_name = "".join(p.capitalize() for p in pascal_parts)
    else:
        # Already in some case format, just capitalize first letter
        table_name = (
            blob_folder_name[0].upper() + blob_folder_name[1:]
            if len(blob_folder_name) > 1
            else blob_folder_name.upper()
        )

    return table_name


def get_schema_for_parquet_file(
    parquet_filename: str, mappings_dir: Optional[Path] = None
) -> Tuple[Optional[Dict], str]:
    """
    Load schema for a parquet file by converting filename to table name.

    Args:
        parquet_filename: Parquet filename (e.g., 'DimMaterial.parquet')
        mappings_dir: Column mappings directory (auto-detected if None)

    Returns:
        Tuple of (schema dict or None, table_name - using the key from schema)
    """
    table_name = parquet_filename_to_table_name(parquet_filename)
    schema = get_schema_for_table(table_name, mappings_dir)

    # If schema not found with PascalCase, try lowercase
    if not schema and table_name:
        schema = get_schema_for_table(table_name.lower(), mappings_dir)

    # Use the actual table name from the schema dict
    if schema:
        actual_table_name = list(schema.keys())[0]
        return schema, actual_table_name

    return schema, table_name


def convert_column_name(parquet_column: str, schema: Dict) -> Optional[str]:
    """
    Convert a parquet column name to database column name using schema.

    Args:
        parquet_column: Parquet column name
        schema: Schema dict with columns info

    Returns:
        Database column name or None if not found
    """
    columns = schema.get("columns", {})

    for parquet_col, col_info in columns.items():
        if parquet_col.lower() == parquet_column.lower():
            return col_info.get("db_column")

    return None


def get_column_mapping(schema: Dict) -> Dict[str, str]:
    """
    Get mapping of parquet column names to database column names.

    Args:
        schema: Schema dict with columns info

    Returns:
        Dict mapping parquet column -> db column
    """
    mapping = {}
    columns = schema.get("columns", {})

    for parquet_col, col_info in columns.items():
        db_col = col_info.get("db_column", parquet_col)
        mapping[parquet_col] = db_col

    return mapping


def validate_schema_file(mapping_file: Path) -> Tuple[bool, list]:
    """
    Validate a column mapping file for required fields.

    Args:
        mapping_file: Path to the mapping file

    Returns:
        Tuple of (is_valid, list of error messages)
    """
    errors = []

    mapping_data = load_column_mapping(mapping_file)
    if not mapping_data:
        return False, ["Failed to parse JSON file"]

    # Check required top-level fields
    if "table_name" not in mapping_data:
        errors.append("Missing 'table_name' field")

    if "columns" not in mapping_data:
        errors.append("Missing 'columns' field")

    # Check columns structure
    columns = mapping_data.get("columns", {})
    if not isinstance(columns, dict):
        errors.append("'columns' must be a dict")
        return False, errors

    for col_name, col_info in columns.items():
        if not isinstance(col_info, dict):
            errors.append(f"Column '{col_name}' info must be a dict")
        elif "db_column" not in col_info:
            errors.append(f"Column '{col_name}' missing 'db_column' field")
        elif "data_type" not in col_info:
            errors.append(f"Column '{col_name}' missing 'data_type' field")

    return len(errors) == 0, errors


def validate_all_schemas(mappings_dir: Optional[Path] = None) -> Dict[str, Tuple[bool, list]]:
    """
    Validate all schema files in the mappings directory.

    Args:
        mappings_dir: Column mappings directory (auto-detected if None)

    Returns:
        Dict mapping filename -> (is_valid, error_list)
    """
    if mappings_dir is None:
        mappings_dir = get_column_mappings_dir()

    validation_results = {}

    for mapping_file in sorted(mappings_dir.glob("*.json")):
        if mapping_file.name in ["INTEGRATION_STATUS.md", "README.md"]:
            continue

        is_valid, errors = validate_schema_file(mapping_file)
        validation_results[mapping_file.name] = (is_valid, errors)

    return validation_results
