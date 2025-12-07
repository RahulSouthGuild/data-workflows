"""
Schema Definitions
Imports all schema definitions (tables, views, matviews) from the schemas/ directory
"""

from db.schemas import TABLES, SCHEMA_TABLES, SCHEMA_VIEWS, SCHEMA_MATVIEWS

__all__ = ["TABLES", "SCHEMA_TABLES", "SCHEMA_VIEWS", "SCHEMA_MATVIEWS"]
