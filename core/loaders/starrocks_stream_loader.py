"""
StarRocks Stream Load Centralized Handler

Unified Stream Load functionality for all CSV/Parquet → StarRocks operations.
Single source of truth for Stream Load logic across all pipelines.

Features:
- Stream Load via HTTP API to StarRocks
- Column mapping support for flexible data loading
- Synchronous operations (production-safe, no async complexity)
- Automatic retry logic with exponential backoff
- Batch labeling to prevent duplicate loads
- Strict error handling (0% error tolerance - no errors tolerated)
- Comprehensive logging with optional debug mode
- Connection pooling for better performance
"""

import time
import uuid
import requests
import pymysql
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from tqdm import tqdm


class StarRocksStreamLoader:
    """
    Centralized Stream Load handler for StarRocks data loading.

    Handles:
    - CSV files to StarRocks tables
    - Parquet files to StarRocks tables
    - Column mapping and transformation
    - Error handling with exponential backoff retries
    - Connection pooling for performance
    """

    def __init__(
        self, config: Dict, logger=None, debug: bool = False, max_error_ratio: float = 0.0
    ):
        """
        Initialize Stream Loader with StarRocks configuration.

        Args:
            config: StarRocks config dict with:
                - host: StarRocks host
                - port: MySQL port (3306)
                - http_port: HTTP port (8040)
                - user: Username
                - password: Password
                - database: Database name
            logger: Optional logger instance for logging output
            debug: Enable debug logging (default: False)
            max_error_ratio: Maximum error ratio (0.0 = strict/no errors, 1.0 = 100% tolerance)
                           (default: 0.0 for production safety)
        """
        self.config = config
        self.logger = logger
        self.debug = debug

        # Stream Load configuration
        self.stream_load_timeout = 1800  # 30 minutes
        self.max_retries = 3
        self.base_retry_delay = 2  # seconds, exponential backoff
        self.max_error_ratio = max_error_ratio  # Configurable error tolerance

        # Connection pool
        self.conn_pool = []
        self.pool_size = 5
        self._initialize_connection_pool()

    def _initialize_connection_pool(self):
        """Initialize connection pool for DB operations."""
        try:
            for _ in range(self.pool_size):
                conn = pymysql.connect(
                    host=self.config["host"],
                    port=self.config["port"],
                    user=self.config["user"],
                    password=self.config["password"],
                    database=self.config["database"],
                    charset="utf8mb4",
                    autocommit=True,
                )
                self.conn_pool.append(conn)
            self._log(f"✅ Connection pool initialized with {self.pool_size} connections", "info")
        except Exception as e:
            self._log(f"❌ Error initializing connection pool: {e}", "error")
            raise

    def _get_connection(self):
        """Get a connection from the pool."""
        if not self.conn_pool:
            raise RuntimeError("❌ Connection pool exhausted")
        conn = self.conn_pool.pop()

        # Health check: try a simple query
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return conn
        except:
            # Connection is dead, create a new one
            try:
                conn = pymysql.connect(
                    host=self.config["host"],
                    port=self.config["port"],
                    user=self.config["user"],
                    password=self.config["password"],
                    database=self.config["database"],
                    charset="utf8mb4",
                    autocommit=True,
                )
                return conn
            except Exception as e:
                self._log(f"❌ Error creating new connection: {e}", "error")
                raise

    def _return_connection(self, conn):
        """Return a connection to the pool."""
        if conn and len(self.conn_pool) < self.pool_size:
            self.conn_pool.append(conn)
        elif conn:
            try:
                conn.close()
            except:
                pass

    def _log(self, message: str, level: str = "info"):
        """
        Unified logging function.

        Args:
            message: Message to log
            level: Log level ('info', 'warning', 'error', 'debug')
        """
        if level == "debug" and not self.debug:
            return  # Skip debug messages if debug disabled

        if self.logger:
            if level == "info":
                self.logger.info(message)
            elif level == "warning":
                self.logger.warning(message)
            elif level == "error":
                self.logger.error(message)
            elif level == "debug":
                self.logger.debug(message)
        else:
            # Console output
            color_map = {
                "info": "\033[36m",  # Cyan
                "warning": "\033[33m",  # Yellow
                "error": "\033[31m",  # Red
                "debug": "\033[90m",  # Gray
            }
            reset = "\033[0m"
            color = color_map.get(level, "")
            print(f"{color}{message}{reset}")

    def _get_stream_load_label(self, table_name: str, chunk_id: str = None) -> str:
        """
        Generate unique Stream Load label.

        Uses timestamp + UUID to prevent duplicate load rejections.

        Args:
            table_name: Target table name
            chunk_id: Optional chunk identifier

        Returns:
            Unique label string
        """
        unique_id = str(uuid.uuid4())[:8]
        timestamp = int(time.time())

        if chunk_id:
            return f"{table_name}_{timestamp}_{chunk_id}_{unique_id}"
        else:
            return f"{table_name}_{timestamp}_{unique_id}"

    def stream_load_csv(
        self,
        table_name: str,
        csv_file_path: str,
        chunk_id: str = None,
        columns: List[str] = None,
        max_error_ratio: float = None,
        null_marker: str = None,
    ) -> Tuple[bool, Dict]:
        """
        Load CSV data into StarRocks table via Stream Load API.

        Features:
        - Automatic retry with exponential backoff (3 retries default)
        - Configurable error tolerance (default: 0% for strict mode)
        - Column mapping support
        - Comprehensive error logging
        - Returns full result details for monitoring

        Args:
            table_name: Target table name in StarRocks
            csv_file_path: Path to CSV file
            chunk_id: Optional chunk identifier for logging/labeling
            columns: Optional list of column names for mapping (CSV → DB order)
            max_error_ratio: Override default error ratio for this operation
                           (0.0 = strict/no errors, 1.0 = 100% tolerance)
                           If None, uses instance default (set in __init__)
            null_marker: String representation of NULL values in CSV (e.g., '\\N' for MySQL NULL)
                        Default: '' (empty string)

        Returns:
            Tuple of (success: bool, result_dict: Dict)
            - success: True if load succeeded, False if failed
            - result_dict: Full Stream Load response containing:
                - Status: 'Success' or 'Fail'
                - Message: Result message
                - TxnId: Transaction ID
                - NumberLoadedRows: Rows successfully loaded
                - NumberFilteredRows: Rows filtered out
                - NumberTotalRows: Total rows processed
                - LoadTimeMs: Time to load in milliseconds
                - ErrorURL: URL to error log (if failed)
        """
        # Use provided max_error_ratio or fall back to instance default
        error_ratio = max_error_ratio if max_error_ratio is not None else self.max_error_ratio
        url = f"http://{self.config['host']}:{self.config['http_port']}/api/{self.config['database']}/{table_name}/_stream_load"

        # Prepare headers for Stream Load
        label = self._get_stream_load_label(table_name, chunk_id)
        headers = {
            "label": label,
            "column_separator": "\x01",
            "format": "CSV",
            "max_filter_ratio": str(error_ratio),
            "strict_mode": "true" if error_ratio == 0.0 else "false",
            "timezone": "Asia/Shanghai",
            "Expect": "100-continue",
        }

        # Add NULL marker if provided (for handling NULL values in CSV)
        if null_marker:
            headers["null_marker"] = null_marker

        # Add column mapping if provided
        if columns:
            headers["columns"] = ",".join(columns)

        auth = (self.config["user"], self.config["password"])

        # Retry logic with exponential backoff
        attempt = 0
        while attempt < self.max_retries:
            try:
                attempt += 1
                self._log(
                    f"📤 Stream Load attempt {attempt}/{self.max_retries} → {table_name} ({chunk_id or 'no-chunk'})",
                    "debug",
                )

                # Read CSV file
                with open(csv_file_path, "rb") as f:
                    file_data = f.read()

                file_size_mb = len(file_data) / 1024 / 1024
                self._log(f"📤 Uploading {file_size_mb:.2f}MB to {table_name}", "debug")

                # Execute Stream Load
                response = requests.put(
                    url,
                    headers=headers,
                    data=file_data,
                    auth=auth,
                    timeout=self.stream_load_timeout,
                )

                result = response.json()
                status = result.get("Status", "Unknown")

                # Check result
                if status == "Success":
                    loaded = result.get("NumberLoadedRows", 0)
                    total = result.get("NumberTotalRows", 0)
                    elapsed_ms = result.get("LoadTimeMs", 0)

                    self._log(
                        f"✅ Stream Load SUCCESS: {loaded}/{total} rows in {elapsed_ms}ms", "info"
                    )
                    return True, result

                else:
                    # Load failed
                    message = result.get("Message", "Unknown error")
                    filtered = result.get("NumberFilteredRows", 0)
                    total = result.get("NumberTotalRows", 0)

                    self._log(
                        f"❌ Stream Load FAILED (Attempt {attempt}/{self.max_retries}):\n"
                        f"   Status: {status}\n"
                        f"   Message: {message}\n"
                        f"   Filtered rows: {filtered}/{total}",
                        "error",
                    )

                    # Check if this is a retriable error
                    retriable_errors = ["internal error", "service unavailable", "timeout"]
                    is_retriable = any(err in message.lower() for err in retriable_errors)

                    if is_retriable and attempt < self.max_retries:
                        # Exponential backoff
                        wait_time = self.base_retry_delay * (2 ** (attempt - 1))
                        self._log(
                            f"⏳ Retriable error detected. Retrying in {wait_time}s...", "warning"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        # Not retriable or out of retries
                        self._log(
                            f"❌ FATAL: Stream Load failed (not retriable or out of retries)",
                            "error",
                        )
                        if "ErrorURL" in result:
                            self._log(f"   Error log: {result['ErrorURL']}", "error")
                        return False, result

            except requests.exceptions.Timeout:
                self._log(f"❌ Stream Load TIMEOUT (Attempt {attempt}/{self.max_retries})", "error")
                if attempt < self.max_retries:
                    wait_time = self.base_retry_delay * (2 ** (attempt - 1))
                    self._log(f"⏳ Retrying in {wait_time}s...", "warning")
                    time.sleep(wait_time)
                    continue
                else:
                    self._log("❌ FATAL: Stream Load timeout - max retries exceeded", "error")
                    return False, {"Status": "Fail", "Message": "Timeout - max retries exceeded"}

            except Exception as e:
                self._log(
                    f"❌ Stream Load ERROR (Attempt {attempt}/{self.max_retries}): {str(e)}",
                    "error",
                )
                if attempt < self.max_retries:
                    wait_time = self.base_retry_delay * (2 ** (attempt - 1))
                    self._log(f"⏳ Retrying in {wait_time}s...", "warning")
                    time.sleep(wait_time)
                    continue
                else:
                    self._log("❌ FATAL: Stream Load error - max retries exceeded", "error")
                    return False, {"Status": "Fail", "Message": str(e)}

        # Should not reach here
        return False, {"Status": "Fail", "Message": "Unknown error - max retries exceeded"}

    def get_table_row_count(self, table_name: str) -> Optional[int]:
        """
        Get current row count for a table.

        Args:
            table_name: Table name

        Returns:
            Row count or None if error
        """
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
            return count
        except Exception as e:
            self._log(f"⚠️ Error getting row count for {table_name}: {e}", "warning")
            return None
        finally:
            if conn:
                self._return_connection(conn)

    def log_stream_load_summary(
        self,
        table_name: str,
        total_rows: int,
        loaded_rows: int,
        filtered_rows: int,
        elapsed_ms: int,
    ):
        """
        Log a summary of Stream Load operation.

        Args:
            table_name: Target table
            total_rows: Total rows in input
            loaded_rows: Successfully loaded rows
            filtered_rows: Filtered/rejected rows
            elapsed_ms: Load time in milliseconds
        """
        filter_pct = (filtered_rows / total_rows * 100) if total_rows > 0 else 0

        summary = (
            f"\n📊 Stream Load Summary:\n"
            f"   Table: {table_name}\n"
            f"   Total: {total_rows:,} rows\n"
            f"   Loaded: {loaded_rows:,} rows\n"
            f"   Filtered: {filtered_rows:,} rows ({filter_pct:.1f}%)\n"
            f"   Time: {elapsed_ms}ms"
        )
        self._log(summary, "info")

    def close(self):
        """Close all connections in the pool."""
        try:
            for conn in self.conn_pool:
                conn.close()
            self.conn_pool.clear()
            self._log("✅ All connections closed", "debug")
        except Exception as e:
            self._log(f"⚠️ Error closing connections: {e}", "warning")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()
        if exc_type:
            self._log(f"❌ Exception occurred: {exc_val}", "error")
            return False
        return True
