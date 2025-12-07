#!/usr/bin/env python3
"""
Simple RLS Mapper - Fetch from StarRocks, Insert to PostgreSQL
1. Fetch email-territory mappings from StarRocks DimHierarchy
2. Store in RAM
3. Insert into PostgreSQL rls_map table
"""

import psycopg2
import sys
from pathlib import Path
from time import perf_counter
import logging

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PG_CONFIG  # noqa: E402

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# StarRocks config (not used - DimHierarchy is in PostgreSQL)
STARROCKS_CONFIG = {
    "host": "localhost",
    "port": 9030,
    "user": "datawiz_admin",
    "password": "0jqhC3X541tP1RmR.5",
    "database": "datawiz",
}

# ANSI colors
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
RED = "\033[31m"
RESET = "\033[0m"


def fetch_from_postgresql():
    """Fetch email-territory mappings from PostgreSQL DimHierarchy."""
    logger.info(f"{BLUE}Connecting to PostgreSQL...{RESET}")
    
    query = """
    SELECT
        email,
        territory_codes,
        territory_count
    FROM
        (
            WITH
                flattened_emails AS (
                    SELECT
                        "WSSTerritoryCode",
                        "SH2Email" AS email
                    FROM
                        "DimHierarchy"
                    WHERE
                        "SH2Email" IS NOT NULL
                        AND "TxnType" LIKE '%Sales Hierarchy%'
                    UNION ALL
                    SELECT
                        "WSSTerritoryCode",
                        "SH3Email" AS email
                    FROM
                        "DimHierarchy"
                    WHERE
                        "SH3Email" IS NOT NULL
                        AND "TxnType" LIKE '%Sales Hierarchy%'
                    UNION ALL
                    SELECT
                        "WSSTerritoryCode",
                        "SH4Email" AS email
                    FROM
                        "DimHierarchy"
                    WHERE
                        "SH4Email" IS NOT NULL
                        AND "TxnType" LIKE '%Sales Hierarchy%'
                    UNION ALL
                    SELECT
                        "WSSTerritoryCode",
                        "SH5Email" AS email
                    FROM
                        "DimHierarchy"
                    WHERE
                        "SH5Email" IS NOT NULL
                        AND "TxnType" LIKE '%Sales Hierarchy%'
                    UNION ALL
                    SELECT
                        "WSSTerritoryCode",
                        "SH6Email" AS email
                    FROM
                        "DimHierarchy"
                    WHERE
                        "SH6Email" IS NOT NULL
                        AND "TxnType" LIKE '%Sales Hierarchy%'
                    UNION ALL
                    SELECT
                        "WSSTerritoryCode",
                        "SH7Email" AS email
                    FROM
                        "DimHierarchy"
                    WHERE
                        "SH7Email" IS NOT NULL
                        AND "TxnType" LIKE '%Sales Hierarchy%'
                )
            SELECT
                TRIM(LOWER(email)) AS email,
                ARRAY_TO_STRING(
                    ARRAY_AGG(DISTINCT "WSSTerritoryCode" ORDER BY "WSSTerritoryCode"),
                    ','
                ) AS territory_codes,
                COUNT(DISTINCT "WSSTerritoryCode") AS territory_count
            FROM
                flattened_emails
            WHERE
                email IS NOT NULL AND TRIM(email) != ''
            GROUP BY
                TRIM(LOWER(email))
            ORDER BY
                email
        ) email_territories
    """
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)
        
        # Fetch all rows as dictionaries
        columns = [desc[0] for desc in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append(dict(zip(columns, row)))
        
        cursor.close()
        conn.close()
        
        logger.info(f"{GREEN}✓ Fetched {len(rows)} records from PostgreSQL{RESET}")
        return rows
        
    except Exception as e:
        logger.error(f"{RED}✗ Error fetching from PostgreSQL: {e}{RESET}")
        raise


def insert_to_postgresql(rows):
    """Insert email-territory mappings to PostgreSQL (one row per email with aggregated territories)."""
    logger.info(f"{BLUE}Connecting to PostgreSQL...{RESET}")
    
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()
        
        # Truncate table
        cursor.execute("TRUNCATE TABLE rls_map")
        conn.commit()
        logger.info(f"{YELLOW}✓ Truncated rls_map{RESET}")
        
        # Insert one row per email with aggregated territories as array
        insert_query = """
        INSERT INTO rls_map (email, wss_territory_code, created_at, updated_at)
        VALUES (%s, %s, NOW(), NOW())
        ON CONFLICT (email) DO UPDATE SET
            wss_territory_code = EXCLUDED.wss_territory_code,
            updated_at = NOW()
        """
        
        inserted = 0
        for row in rows:
            email = row["email"]
            territories_str = row["territory_codes"]
            
            # Parse comma-separated territories into array
            if territories_str:
                territory_list = [t.strip() for t in territories_str.split(",")]
                # Create PostgreSQL array format
                territories_array = territory_list
                cursor.execute(insert_query, (email, territories_array))
                inserted += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"{GREEN}✓ Inserted {inserted} email records{RESET}")
        return inserted
        
    except Exception as e:
        logger.error(f"{RED}✗ Error inserting to PostgreSQL: {e}{RESET}")
        raise


def main():
    """Main execution."""
    start_time = perf_counter()
    
    logger.info(f"{BLUE}{'='*60}")
    logger.info("RLS Mapper - StarRocks to PostgreSQL")
    logger.info(f"{'='*60}{RESET}")
    
    # Step 1: Fetch from PostgreSQL
    logger.info(f"{BLUE}Step 1: Fetching from PostgreSQL...{RESET}")
    rows = fetch_from_postgresql()
    
    # Step 2: Insert to PostgreSQL
    logger.info(f"{BLUE}Step 2: Inserting to PostgreSQL...{RESET}")
    inserted = insert_to_postgresql(rows)
    
    elapsed = perf_counter() - start_time
    logger.info(f"{GREEN}{'='*60}")
    logger.info(f"✓ Completed in {elapsed:.2f}s")
    logger.info(f"  Total emails: {len(rows)}")
    logger.info(f"  Total inserted: {inserted}")
    logger.info(f"{'='*60}{RESET}")


if __name__ == "__main__":
    main()
