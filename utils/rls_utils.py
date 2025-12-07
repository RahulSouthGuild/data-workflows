"""
Row Level Security (RLS) Mapping Utilities

Handles RLS data processing and mapping for dimension tables,
particularly DimHierarchy to RlsMaster mapping.
"""

import logging
from typing import Optional, List, Dict, Any
from utils.starrocks_utils import execute_query, execute_update, truncate_table
from utils.dim_transform_utils import extract_email_hierarchy
from tqdm import tqdm

# Color codes for console output
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
RESET = "\033[0m"


def fetch_hierarchy_data(logger: Optional[logging.Logger] = None) -> List[Dict[str, Any]]:
    """Fetch hierarchy data from DimHierarchy table for RLS mapping

    Returns:
        List of dictionaries with WSSTerritoryCode and email fields

    Raises:
        Exception: If query fails
    """
    try:
        if logger:
            logger.info("📋 Fetching hierarchy data from DimHierarchy...")

        query = """
        SELECT
            "WSSTerritoryCode",
            "SH2Email",
            "SH3Email",
            "SH4Email",
            "SH5Email",
            "SH6Email",
            "SH7Email"
        FROM "DimHierarchy"
        WHERE "TxnType" LIKE '%Sales Hierarchy%'
        """

        rows = execute_query(query, logger)

        # Convert to list of dicts for easier processing
        hierarchy_data = []
        for row in rows:
            hierarchy_data.append(
                {
                    "WSSTerritoryCode": row[0],
                    "emails": [row[i] for i in range(1, 7) if i < len(row)],
                }
            )

        if logger:
            logger.info(f"{GREEN}✓ Fetched {len(hierarchy_data):,} hierarchy records{RESET}")

        return hierarchy_data

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ Failed to fetch hierarchy data: {e}{RESET}")
        raise


def process_rls_records(
    hierarchy_data: List[Dict[str, Any]],
    logger: Optional[logging.Logger] = None,
) -> List[tuple]:
    """Process hierarchy data into RLS records

    Extracts email paths and builds sanitized mapping.

    Args:
        hierarchy_data: List of hierarchy dictionaries
        logger: Optional logger instance

    Returns:
        List of tuples (WSSTerritoryCode, email_path) ready for insertion

    Example:
        records = process_rls_records(hierarchy_data)
        # Results in: [
        #   ('WCODE1', 'user1_at_company_dot_com.user2_at_company_dot_com'),
        #   ...
        # ]
    """
    try:
        if logger:
            logger.info(f"🔄 Processing {len(hierarchy_data):,} records for RLS mapping...")

        rls_records = []
        batch_size = 1000
        processed = 0

        with tqdm(
            total=len(hierarchy_data),
            desc="Processing RLS records",
            ncols=100,
            unit="records",
        ) as pbar:
            for record in hierarchy_data:
                wss_code = record["WSSTerritoryCode"]
                email_path = extract_email_hierarchy(record["emails"])

                if email_path:
                    rls_records.append((wss_code, email_path))

                processed += 1
                pbar.update(1)

                if processed % batch_size == 0:
                    pbar.set_postfix({"Processed": f"{processed:,}"})

        if logger:
            logger.info(f"{GREEN}✓ Processed {len(rls_records):,} valid RLS records{RESET}")

        return rls_records

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ RLS record processing failed: {e}{RESET}")
        raise


def insert_rls_records(
    rls_records: List[tuple],
    batch_size: int = 1000,
    logger: Optional[logging.Logger] = None,
) -> int:
    """Insert RLS records into RlsMaster table in batches

    Args:
        rls_records: List of (WSSTerritoryCode, email_path) tuples
        batch_size: Number of records to insert per batch
        logger: Optional logger instance

    Returns:
        Total number of records inserted

    Raises:
        Exception: If insertion fails
    """
    try:
        if logger:
            logger.info(f"📥 Inserting {len(rls_records):,} RLS records...")

        total_inserted = 0
        total_batches = (len(rls_records) + batch_size - 1) // batch_size

        with tqdm(
            total=len(rls_records),
            desc="Inserting RLS records",
            ncols=100,
            unit="records",
        ) as pbar:
            for batch_num, i in enumerate(range(0, len(rls_records), batch_size), 1):
                batch = rls_records[i : i + batch_size]

                # Build INSERT statement
                values_list = [f"('{record[0]}', '{record[1]}')" for record in batch]
                values_str = ", ".join(values_list)

                insert_query = f"""
                INSERT INTO "RlsMaster" ("WSSTerritoryCode", "Path")
                VALUES {values_str}
                """

                execute_update(insert_query, logger)
                total_inserted += len(batch)
                pbar.update(len(batch))
                pbar.set_postfix(
                    {
                        "Batch": f"{batch_num}/{total_batches}",
                        "Total": f"{total_inserted:,}",
                    }
                )

        if logger:
            logger.info(f"{GREEN}✓ Successfully inserted {total_inserted:,} RLS records{RESET}")

        return total_inserted

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ RLS record insertion failed: {e}{RESET}")
        raise


def update_rls_master(logger: Optional[logging.Logger] = None) -> int:
    """Main orchestrator for updating RLS Master table

    Workflow:
    1. Truncate existing RlsMaster
    2. Fetch hierarchy data from DimHierarchy
    3. Process into RLS records
    4. Insert into RlsMaster

    Args:
        logger: Optional logger instance

    Returns:
        Total number of records inserted

    Raises:
        Exception: If any step fails
    """
    try:
        if logger:
            logger.info("🔄 Starting RLS Master update process...")

        # Step 1: Truncate
        if not truncate_table("RlsMaster", logger):
            raise Exception("Failed to truncate RlsMaster")

        # Step 2: Fetch hierarchy data
        hierarchy_data = fetch_hierarchy_data(logger)

        # Step 3: Process into RLS records
        rls_records = process_rls_records(hierarchy_data, logger)

        # Step 4: Insert records
        total_inserted = insert_rls_records(rls_records, logger=logger)

        if logger:
            logger.info(f"{GREEN}✅ RLS Master update complete - {total_inserted:,} records{RESET}")

        return total_inserted

    except Exception as e:
        if logger:
            logger.error(f"{RED}❌ RLS Master update failed: {e}{RESET}")
        raise
