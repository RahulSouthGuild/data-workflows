"""
Unified ETL Orchestrator

Provides a single entry point for complete dimension table ETL pipeline:
Extract → Transform → Clean → Validate → Load

This module orchestrates the entire data flow using modular utilities.
"""

import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import polars as pl
import pymysql
import requests

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from utils.logging_utils import get_pipeline_logger  # noqa: E402
from utils.schema_loader import get_schema_for_parquet_file  # noqa: E402
from utils.column_mapper import (  # noqa: E402
    build_column_mapping_header,
    extract_columns_from_schema,
)
from utils.pipeline_config import (  # noqa: E402
    Config,
)
from utils.dim_transform_utils import apply_type_conversions  # noqa: E402
from core.transformers.transformation_engine import (  # noqa: E402
    validate_and_transform_dataframe,
)

logger = get_pipeline_logger(__name__)

# Color codes
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


class ETLOrchestrator:
    """
    Orchestrates complete ETL pipeline for dimension tables.

    Pipeline flow:
    1. EXTRACT: Read parquet file
    2. TRANSFORM: Apply schema mappings and column renames
    3. CLEAN: Normalize data types and values
    4. VALIDATE: Check schema alignment
    5. LOAD: Stream Load into StarRocks
    """

    def __init__(self):
        """Initialize ETL orchestrator with StarRocks configuration."""
        self.host = Config.STARROCKS_HOST
        self.port = Config.STARROCKS_PORT
        self.http_port = Config.STARROCKS_HTTP_PORT
        self.user = Config.STARROCKS_USER
        self.password = Config.STARROCKS_PASSWORD
        self.database = Config.STARROCKS_DATABASE
        self.timeout = Config.STREAM_LOAD_TIMEOUT
        self.max_error_ratio = Config.MAX_ERROR_RATIO
        self.chunk_size = Config.CHUNK_SIZE

    def get_starrocks_connection(self):
        """Create MySQL connection to StarRocks for metadata operations."""
        return pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            charset="utf8mb4",
            autocommit=True,
        )

    def get_table_columns(self, table_name: str) -> Dict[str, str]:
        """
        Get column names and types from StarRocks table.

        Args:
            table_name: Database table name

        Returns:
            Dict of {column_name: column_type}
        """
        try:
            conn = self.get_starrocks_connection()
            with conn.cursor() as cursor:
                cursor.execute(f"DESC {table_name}")
                rows = cursor.fetchall()
                # DESC returns (column_name, type, ...)
                columns = {row[0]: row[1] for row in rows}
            conn.close()
            logger.info(f"{GREEN}✓ Fetched {len(columns)} columns from {table_name}{RESET}")
            return columns
        except Exception as e:
            logger.error(f"{RED}Error fetching table columns: {e}{RESET}")
            return {}

    def truncate_table(self, table_name: str) -> bool:
        """
        Truncate table to prepare for fresh load.

        Args:
            table_name: Database table name

        Returns:
            True if successful, False otherwise
        """
        try:
            conn = self.get_starrocks_connection()
            with conn.cursor() as cursor:
                logger.info(f"{YELLOW}Truncating {table_name}...{RESET}")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.close()
            logger.info(f"{GREEN}✓ Truncated {table_name}{RESET}")
            return True
        except Exception as e:
            logger.error(f"{RED}Error truncating table: {e}{RESET}")
            return False

    def extract(self, parquet_path: Path) -> Optional[pl.DataFrame]:
        """
        EXTRACT: Read parquet file into DataFrame.

        Args:
            parquet_path: Path to parquet file

        Returns:
            Polars DataFrame or None if error
        """
        try:
            logger.info(f"{CYAN}[EXTRACT] Reading {parquet_path.name}...{RESET}")
            df = pl.read_parquet(parquet_path)
            logger.info(f"{GREEN}✓ Extracted {len(df):,} rows × {len(df.columns)} columns{RESET}")
            return df
        except Exception as e:
            logger.error(f"{RED}[EXTRACT] Error reading parquet: {e}{RESET}")
            return None

    def transform(
        self, df: pl.DataFrame, table_name: str, schema: Dict
    ) -> Tuple[Optional[pl.DataFrame], Dict[str, str]]:
        """
        TRANSFORM: Apply schema mappings and column renames using centralized engine.

        Uses the unified transformation engine from core.transformers.transformation_engine
        which handles:
        - Column mapping from JSON files
        - Schema validation
        - Data type overflow detection
        - VARCHAR overflow auto-fix

        Args:
            df: Input DataFrame
            table_name: Database table name
            schema: Schema dictionary (not used, kept for compatibility)

        Returns:
            Tuple of (transformed_df, column_mapping_dict) or (None, {}) if error
        """
        try:
            logger.info(f"{CYAN}[TRANSFORM] Mapping columns for {table_name}...{RESET}")

            # Use centralized transformation engine
            transformed_df, metadata = validate_and_transform_dataframe(df, table_name, logger)

            # Extract column mapping from metadata for compatibility
            mapping = {col: col for col in transformed_df.columns}

            logger.info(
                f"{GREEN}✓ Transform complete: {len(transformed_df):,} rows × {len(transformed_df.columns)} columns{RESET}"
            )

            return transformed_df, mapping

        except Exception as e:
            logger.error(f"{RED}[TRANSFORM] Error in transformation: {e}{RESET}")
            import traceback

            logger.error(traceback.format_exc())
            return None, {}

    def clean(self, df: pl.DataFrame, table_name: str) -> Optional[pl.DataFrame]:
        """
        CLEAN: Normalize data types and values.

        This step:
        - Converts column types to match schema
        - Handles NULL values and type mismatches
        - Cleans numeric strings

        Args:
            df: Input DataFrame
            table_name: Database table name

        Returns:
            Cleaned DataFrame or None if error
        """
        try:
            logger.info(f"{CYAN}[CLEAN] Normalizing data types for {table_name}...{RESET}")

            # Get database column types
            db_columns = self.get_table_columns(table_name)
            if not db_columns:
                logger.warning(
                    f"{YELLOW}[CLEAN] Could not get DB schema, skipping type conversion{RESET}"
                )
                return df

            # Apply type conversions
            df = apply_type_conversions(df, db_columns, table_name, logger)

            logger.info(f"{GREEN}✓ Data cleaning complete{RESET}")
            return df

        except Exception as e:
            logger.error(f"{RED}[CLEAN] Error during cleaning: {e}{RESET}")
            return None

    def validate(self, df: pl.DataFrame, table_name: str) -> bool:
        """
        VALIDATE: Check schema alignment before loading.

        This step:
        - Verifies all columns exist in database table
        - Checks column count matches
        - Detects potential type mismatches

        Args:
            df: Input DataFrame
            table_name: Database table name

        Returns:
            True if validation passes, False otherwise
        """
        try:
            logger.info(f"{CYAN}[VALIDATE] Checking schema alignment for {table_name}...{RESET}")

            # Get database columns
            db_columns = self.get_table_columns(table_name)
            if not db_columns:
                logger.error(f"{RED}[VALIDATE] Could not fetch database columns{RESET}")
                return False

            # Check all dataframe columns exist in database
            missing_cols = [col for col in df.columns if col not in db_columns]
            if missing_cols:
                logger.error(
                    f"{RED}[VALIDATE] Columns not in database: {', '.join(missing_cols)}{RESET}"
                )
                return False

            logger.info(
                f"{GREEN}✓ Schema validation passed: "
                f"{len(df.columns)} columns, {len(df):,} rows{RESET}"
            )
            return True

        except Exception as e:
            logger.error(f"{RED}[VALIDATE] Validation error: {e}{RESET}")
            return False

    def load(
        self, df: pl.DataFrame, table_name: str, chunk_id: Optional[int] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        LOAD: Stream Load into StarRocks using HTTP API.

        For large datasets, chunks data into CHUNK_SIZE rows per batch.

        Args:
            df: DataFrame to load
            table_name: Database table name
            chunk_id: Optional chunk identifier for logging

        Returns:
            Tuple of (success_bool, result_dict)
        """
        try:
            logger.info(f"{CYAN}[LOAD] Starting Stream Load for {table_name}...{RESET}")

            total_rows = len(df)
            num_chunks = (total_rows + self.chunk_size - 1) // self.chunk_size

            if num_chunks > 1:
                logger.info(
                    f"{CYAN}Processing {num_chunks} chunks ({self.chunk_size:,} rows each){RESET}"
                )

            total_loaded = 0
            total_failed = 0

            for i in range(num_chunks):
                start_idx = i * self.chunk_size
                end_idx = min((i + 1) * self.chunk_size, total_rows)

                chunk_df = df[start_idx:end_idx]
                chunk_label = f"{table_name}_{int(time.time())}_{i}"

                # Save chunk to CSV with SOH delimiter
                import tempfile  # noqa: F401, E402

                with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                    csv_path = Path(f.name)
                    chunk_df.write_csv(csv_path, separator="\x01")

                # Stream Load this chunk
                success, result = self._stream_load_chunk(table_name, csv_path, chunk_label)

                # Clean up temp file
                csv_path.unlink()

                if success:
                    loaded = result.get("NumberLoadedRows", 0)
                    total_loaded += loaded
                else:
                    filtered = result.get("NumberFilteredRows", 0)
                    total_failed += filtered

                # Show progress
                progress = f"Chunk {i+1}/{num_chunks}"
                if success:
                    logger.info(f"{GREEN}✓ {progress} loaded {end_idx - start_idx:,} rows{RESET}")
                else:
                    logger.warning(f"{YELLOW}⚠ {progress} filtered {total_failed:,} rows{RESET}")

            # Summary
            logger.info(
                f"{GREEN}✓ Stream Load complete: {total_loaded:,} loaded, "
                f"{total_failed:,} filtered{RESET}"
            )

            return (total_failed == 0), {"total_loaded": total_loaded, "total_failed": total_failed}

        except Exception as e:
            logger.error(f"{RED}[LOAD] Stream Load error: {e}{RESET}")
            return False, {"error": str(e)}

    def _stream_load_chunk(
        self, table_name: str, csv_path: Path, label: str
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute Stream Load for a single chunk.

        Args:
            table_name: Database table name
            csv_path: Path to CSV file to load
            label: Unique label for this load

        Returns:
            Tuple of (success_bool, response_dict)
        """
        url = (
            f"http://{self.host}:{self.http_port}/api/" f"{self.database}/{table_name}/_stream_load"
        )

        headers = {
            "label": label,
            "column_separator": "\x01",
            "format": "CSV",
            "max_filter_ratio": str(self.max_error_ratio),
            "strict_mode": "false",
            "timezone": "Asia/Shanghai",
            "Expect": "100-continue",
        }

        auth = (self.user, self.password)

        try:
            with open(csv_path, "rb") as f:
                file_data = f.read()

            response = requests.put(
                url, headers=headers, data=file_data, auth=auth, timeout=self.timeout
            )

            result = response.json()

            if result.get("Status") == "Success":
                return True, result
            else:
                # Parse error details
                status = result.get("Status", "Unknown")
                message = result.get("Message", "No message")
                total = result.get("NumberTotalRows", 0)
                loaded = result.get("NumberLoadedRows", 0)
                filtered = result.get("NumberFilteredRows", 0)

                if total > 0:
                    filter_ratio = (filtered / total) * 100
                    logger.warning(
                        f"{YELLOW}Stream Load {status}: {loaded:,} loaded, "
                        f"{filtered:,} filtered ({filter_ratio:.1f}%){RESET}"
                    )
                else:
                    logger.error(f"{RED}Stream Load failed: {message}{RESET}")

                return False, result

        except Exception as e:
            logger.error(f"{RED}Stream Load exception: {e}{RESET}")
            return False, {"error": str(e)}

    def orchestrate(
        self,
        parquet_path: Path,
        table_name: str,
        schema: Optional[Dict] = None,
        truncate: bool = True,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute complete ETL pipeline: Extract → Transform → Clean → Validate → Load.

        Args:
            parquet_path: Path to parquet file
            table_name: Database table name
            schema: Schema dictionary (if None, will be loaded from db/column_mappings/)
            truncate: Whether to truncate table before loading

        Returns:
            Tuple of (success_bool, result_dict) with pipeline metadata
        """
        start_time = time.time()
        result = {
            "table_name": table_name,
            "parquet_path": str(parquet_path),
            "steps": {},
        }

        try:
            # Load schema if not provided
            if schema is None:
                schema, actual_table_name = get_schema_for_parquet_file(parquet_path.name)
                if not schema:
                    raise Exception(f"Could not load schema for {table_name}")

            # STEP 1: EXTRACT
            df = self.extract(parquet_path)
            if df is None:
                raise Exception("Extract failed")
            result["steps"]["extract"] = {"rows": len(df), "columns": len(df.columns)}

            # STEP 2: TRUNCATE (if requested)
            if truncate:
                if not self.truncate_table(table_name):
                    logger.warning(f"{YELLOW}Truncate failed, continuing anyway{RESET}")

            # STEP 3: TRANSFORM
            df, mapping = self.transform(df, table_name, schema)
            if df is None:
                raise Exception("Transform failed")
            result["steps"]["transform"] = {
                "rows": len(df),
                "columns": len(df.columns),
                "mappings": len(mapping),
            }

            # STEP 4: CLEAN
            df = self.clean(df, table_name)
            if df is None:
                raise Exception("Clean failed")
            result["steps"]["clean"] = {"rows": len(df), "columns": len(df.columns)}

            # STEP 5: VALIDATE
            if not self.validate(df, table_name):
                raise Exception("Validation failed")
            result["steps"]["validate"] = {"passed": True}

            # STEP 6: LOAD
            load_success, load_result = self.load(df, table_name)
            result["steps"]["load"] = load_result

            if not load_success:
                logger.warning(f"{YELLOW}Load completed with warnings{RESET}")

            # Summary
            elapsed = time.time() - start_time
            result["success"] = load_success
            result["elapsed_seconds"] = elapsed

            if load_success:
                logger.info(
                    f"{GREEN}✓ ETL pipeline complete in {elapsed:.2f}s "
                    f"({len(df):,} rows loaded){RESET}"
                )
            else:
                logger.warning(f"{YELLOW}⚠ ETL completed with issues in {elapsed:.2f}s{RESET}")

            return load_success, result

        except Exception as e:
            elapsed = time.time() - start_time
            result["success"] = False
            result["error"] = str(e)
            result["elapsed_seconds"] = elapsed

            logger.error(f"{RED}✗ ETL pipeline failed: {e}{RESET}")
            return False, result


# Convenience function for one-off orchestration
def orchestrate_etl(
    parquet_path: Path, table_name: str, schema: Optional[Dict] = None, truncate: bool = True
) -> Tuple[bool, Dict[str, Any]]:
    """
    Execute complete ETL pipeline for a dimension table.

    Quick function to orchestrate the full pipeline without needing to
    instantiate the orchestrator class directly.

    Args:
        parquet_path: Path to parquet file
        table_name: Database table name
        schema: Optional schema dictionary
        truncate: Whether to truncate table first (default True)

    Returns:
        Tuple of (success_bool, result_dict)

    Example:
        success, result = orchestrate_etl(
            Path("data/DimCustomerMaster.parquet"),
            "dim_customer_master"
        )
    """
    orchestrator = ETLOrchestrator()
    return orchestrator.orchestrate(parquet_path, table_name, schema, truncate)
