"""
StarRocks Database Utilities

Provides connection management, Stream Load operations, and query execution
for StarRocks. Uses the HTTP Stream Load API for efficient bulk inserts.
"""

import time
import os
import requests
import pymysql
from typing import Optional, Dict, List, Tuple, Any
import logging

from utils.DB_CONFIG import DB_CONFIG

# Color codes for console output
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"

# Stream Load settings
STREAM_LOAD_TIMEOUT = 1800  # 30 minutes
MAX_ERROR_RATIO = 0.1  # 10% error tolerance
CHUNK_SIZE = 100000  # Records per chunk


class StarRocksConnection:
    """Context manager for StarRocks MySQL connections"""

    def __init__(self, config: Dict = None):
        self.config = config or DB_CONFIG
        self.conn = None

    def __enter__(self):
        self.conn = pymysql.connect(
            host=self.config["host"],
            port=self.config.get("port", 9030),
            user=self.config["user"],
            password=self.config["password"],
            database=self.config["database"],
            charset=self.config.get("charset", "utf8mb4"),
            autocommit=self.config.get("autocommit", True),
        )
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()


def get_starrocks_connection() -> pymysql.Connection:
    """Create and return a StarRocks MySQL connection

    Returns:
        pymysql.Connection: Active database connection

    Raises:
        pymysql.Error: If connection fails
    """
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG.get("port", 9030),
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        charset=DB_CONFIG.get("charset", "utf8mb4"),
        autocommit=DB_CONFIG.get("autocommit", True),
    )


def execute_query(query: str, logger: Optional[logging.Logger] = None) -> List[Tuple]:
    """Execute a SELECT query against StarRocks

    Args:
        query: SQL SELECT query
        logger: Optional logger instance

    Returns:
        List of tuples containing query results

    Raises:
        pymysql.Error: If query execution fails
    """
    conn = None
    try:
        conn = get_starrocks_connection()
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()
    except Exception as e:
        if logger:
            logger.error(f"❌ Query execution failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def execute_update(query: str, logger: Optional[logging.Logger] = None) -> int:
    """Execute an UPDATE/DELETE/INSERT query against StarRocks

    Args:
        query: SQL DML query
        logger: Optional logger instance

    Returns:
        Number of affected rows

    Raises:
        pymysql.Error: If query execution fails
    """
    conn = None
    try:
        conn = get_starrocks_connection()
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
            return cursor.rowcount
    except Exception as e:
        if logger:
            logger.error(f"❌ Update execution failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def get_table_columns(table_name: str, logger: Optional[logging.Logger] = None) -> List[str]:
    """Get column names from a StarRocks table

    Args:
        table_name: Name of the table
        logger: Optional logger instance

    Returns:
        List of column names in table order

    Raises:
        pymysql.Error: If query fails
    """
    try:
        rows = execute_query(f"DESC {table_name}", logger)
        return [row[0] for row in rows]
    except Exception as e:
        if logger:
            logger.error(f"❌ Error getting table columns for {table_name}: {e}")
        return []


def get_table_schema(table_name: str, logger: Optional[logging.Logger] = None) -> Dict[str, str]:
    """Get full schema (column name -> data type) for a StarRocks table

    Args:
        table_name: Name of the table
        logger: Optional logger instance

    Returns:
        Dictionary mapping column names to their data types

    Raises:
        pymysql.Error: If query fails
    """
    try:
        rows = execute_query(f"DESC {table_name}", logger)
        return {row[0]: row[1] for row in rows}
    except Exception as e:
        if logger:
            logger.error(f"❌ Error getting table schema for {table_name}: {e}")
        return {}


def truncate_table(table_name: str, logger: Optional[logging.Logger] = None) -> bool:
    """Truncate all records from a StarRocks table

    Args:
        table_name: Name of the table to truncate
        logger: Optional logger instance

    Returns:
        True if successful, False otherwise
    """
    try:
        if logger:
            logger.info(f"{YELLOW}Truncating {table_name}...{RESET}")
        execute_update(f"TRUNCATE TABLE {table_name}", logger)
        if logger:
            logger.info(f"{GREEN}✓ Truncated {table_name}{RESET}")
        return True
    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Error truncating {table_name}: {e}{RESET}")
        return False


def stream_load_csv(
    table_name: str,
    csv_file_path: str,
    chunk_id: Optional[int] = None,
    columns: Optional[List[str]] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[bool, Dict]:
    """Load CSV data into StarRocks using Stream Load HTTP API

    Args:
        table_name: Target table name
        csv_file_path: Path to CSV file
        chunk_id: Optional chunk identifier for logging
        columns: List of column names for CSV->DB mapping (in order)
        logger: Optional logger instance

    Returns:
        Tuple of (success: bool, response_json: dict)
    """
    # Prepare the Stream Load URL
    url = f"http://{DB_CONFIG['host']}:{os.getenv('STARROCKS_HTTP_PORT', '8040')}/api/{DB_CONFIG['database']}/{table_name}/_stream_load"

    # Prepare headers
    headers = {
        "label": f"{table_name}_{int(time.time())}_{chunk_id if chunk_id else ''}",
        "column_separator": "\x01",
        "format": "CSV",
        "max_filter_ratio": str(MAX_ERROR_RATIO),
        "strict_mode": "false",
        "timezone": "Asia/Shanghai",
        "Expect": "100-continue",
    }

    # Add columns specification if provided (CRITICAL for correct column mapping!)
    if columns:
        headers["columns"] = ",".join(columns)

    # Authentication
    auth = (DB_CONFIG["user"], DB_CONFIG["password"])

    try:
        # Read the file
        with open(csv_file_path, "rb") as f:
            file_data = f.read()

        # Execute Stream Load
        response = requests.put(
            url, headers=headers, data=file_data, auth=auth, timeout=STREAM_LOAD_TIMEOUT
        )

        # Parse response
        result = response.json()

        if result.get("Status") == "Success":
            return True, result
        else:
            # Get detailed error information
            status = result.get("Status", "Unknown")
            message = result.get("Message", "No message provided")
            total = result.get("NumberTotalRows", 0)
            loaded = result.get("NumberLoadedRows", 0)
            filtered = result.get("NumberFilteredRows", 0)

            # Check if this is a critical failure
            if filtered > 0 and total > 0:
                filter_ratio = (filtered / total) * 100
                is_critical = filter_ratio > (MAX_ERROR_RATIO * 100)
            else:
                is_critical = status == "Fail"

            # Log error with appropriate severity
            error_color = RED if is_critical else YELLOW
            error_type = "ERROR" if is_critical else "WARNING"

            if logger:
                logger.warning(
                    f"{error_color}❌ Stream Load {error_type} for chunk {chunk_id}:{RESET}"
                )
                logger.warning(f"{error_color}  Status: {status}{RESET}")
                logger.warning(f"{error_color}  Message: {message}{RESET}")

                if total > 0:
                    filter_percentage = (filtered / total) * 100 if total > 0 else 0
                    logger.warning(f"{error_color}  Total Rows: {total:,}{RESET}")
                    logger.warning(f"{error_color}  Loaded Rows: {loaded:,}{RESET}")
                    logger.warning(
                        f"{error_color}  Filtered Rows: {filtered:,} ({filter_percentage:.1f}%){RESET}"
                    )

                    if filtered > 0:
                        logger.warning(f"{error_color}  ⚠️  High filter ratio detected!{RESET}")

            return False, result

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Stream Load Exception: {str(e)}{RESET}")
        return False, {"Message": str(e)}


def validate_column_mapping(
    csv_columns: List[str],
    table_name: str,
    logger: Optional[logging.Logger] = None,
) -> Dict[str, Any]:
    """Validate CSV columns against database table schema

    Args:
        csv_columns: List of CSV column names
        table_name: Target table name
        logger: Optional logger instance

    Returns:
        Dictionary with validation results and ordered columns

    Example:
        result = validate_column_mapping(['col1', 'col2'], 'my_table')
        if result['valid']:
            ordered_cols = result['ordered_columns']
    """
    try:
        db_columns = get_table_columns(table_name, logger)
        schema = get_table_schema(table_name, logger)

        if not db_columns:
            return {
                "valid": False,
                "error": f"Could not fetch schema for {table_name}",
                "ordered_columns": csv_columns,
            }

        csv_columns_lower = {col.lower(): col for col in csv_columns}
        missing_in_csv = set(db_columns) - set(csv_columns_lower.keys())
        extra_in_csv = set(csv_columns_lower.keys()) - set(col.lower() for col in db_columns)

        ordered_columns = []
        for db_col in db_columns:
            db_col_lower = db_col.lower()
            if db_col_lower in csv_columns_lower:
                ordered_columns.append(csv_columns_lower[db_col_lower])

        if logger:
            logger.info(f"{GREEN}✓ Column mapping verified{RESET}")
            if missing_in_csv:
                logger.warning(f"{YELLOW}  ⚠️  Missing columns in CSV: {missing_in_csv}{RESET}")
            if extra_in_csv:
                logger.warning(f"{YELLOW}  ⚠️  Extra columns in CSV: {extra_in_csv}{RESET}")

        return {
            "valid": len(ordered_columns) > 0,
            "ordered_columns": ordered_columns,
            "missing_in_csv": missing_in_csv,
            "extra_in_csv": extra_in_csv,
            "schema": schema,
        }

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Column mapping validation failed: {e}{RESET}")
        return {
            "valid": False,
            "error": str(e),
            "ordered_columns": csv_columns,
        }
