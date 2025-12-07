"""
Schema Definitions Loader
Loads all table, view, and materialized view definitions from subdirectories
"""

from pathlib import Path
import importlib.util


def load_schema_from_file(filepath):
    """Load schema definition (table, view, or matview) from a Python file"""
    spec = importlib.util.spec_from_file_location("schema_module", filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TABLE


def load_schemas_from_folder(folder_name):
    """Load all schemas from a specific folder"""
    schemas_dir = Path(__file__).parent
    folder_path = schemas_dir / folder_name
    schemas = []

    if not folder_path.exists():
        return schemas

    # Get all Python files except __init__.py
    for filepath in sorted(folder_path.glob("*.py")):
        if filepath.name == "__init__.py":
            continue

        try:
            schema = load_schema_from_file(filepath)
            schemas.append(schema)
        except Exception as e:
            print(f"Error loading {filepath.name}: {e}")

    return schemas


def load_all_schemas():
    """Load all schemas from tables/, views/, and matviews/ folders"""
    all_schemas = []

    # Load tables
    tables = load_schemas_from_folder("tables")
    all_schemas.extend(tables)

    # Load views
    views = load_schemas_from_folder("views")
    all_schemas.extend(views)

    # Load materialized views
    matviews = load_schemas_from_folder("matviews")
    all_schemas.extend(matviews)

    # Sort by order
    all_schemas.sort(key=lambda x: x.get("order", 999))

    return all_schemas


def get_tables():
    """Get only table definitions"""
    all_schemas = load_all_schemas()
    return [s for s in all_schemas if not s.get("type")]


def get_views():
    """Get only view definitions"""
    all_schemas = load_all_schemas()
    return [s for s in all_schemas if s.get("type") == "VIEW"]


def get_matviews():
    """Get only materialized view definitions"""
    all_schemas = load_all_schemas()
    return [s for s in all_schemas if s.get("type") == "MATVIEW"]


# Export all schemas
TABLES = load_all_schemas()
SCHEMA_TABLES = get_tables()
SCHEMA_VIEWS = get_views()
SCHEMA_MATVIEWS = get_matviews()
