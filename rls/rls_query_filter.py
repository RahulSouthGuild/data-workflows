#!/usr/bin/env python3
"""
RLS Query Filter - Dynamic WHERE clause injection using sqlglot
1. Takes a query (with wss_territory_code column)
2. Asks for email
3. Looks up territory codes from rls_map
4. Uses sqlglot to add WHERE clause filtering
5. Executes query in StarRocks
6. Shows before/after query
"""

import sqlglot
import pymysql
import psycopg2
import sys
from pathlib import Path
import logging

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import PG_CONFIG  # noqa: E402

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# StarRocks config
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
CYAN = "\033[36m"
RESET = "\033[0m"


def get_territories_by_email(email: str) -> list:
    """
    Get territory codes for a given email from rls_map.

    Args:
        email: Email address to lookup

    Returns:
        List of territory codes
    """
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cursor = conn.cursor()

        query = "SELECT wss_territory_code FROM rls_map WHERE email = %s"
        cursor.execute(query, (email.lower().strip(),))
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        if result:
            territories = result[0]  # PostgreSQL array
            if isinstance(territories, list):
                return territories
            return []

        logger.warning(f"{YELLOW}⚠ No territories found for email: {email}{RESET}")
        return []

    except Exception as e:
        logger.error(f"{RED}✗ Error looking up territories: {e}{RESET}")
        return []


def add_rls_filter(
    query: str, territories: list, territory_column: str = "wss_territory_code"
) -> str:
    """
    Add RLS filter to query using sqlglot.

    Args:
        query: Original SQL query
        territories: List of territory codes to filter by
        territory_column: Column name to filter on (default: wss_territory_code)

    Returns:
        Modified query with RLS filter
    """
    try:
        # Parse the query
        parsed = sqlglot.parse_one(query, read="mysql")

        # Create IN clause string: wss_territory_code IN ('T001', 'T002', ...)
        territory_values = ", ".join([f"'{t}'" for t in territories])
        in_clause = f"{territory_column} IN ({territory_values})"

        # Parse the RLS condition
        rls_condition = sqlglot.parse_one(in_clause, read="mysql", into=sqlglot.exp.Expression)

        # Get existing WHERE clause
        where_clause = parsed.find(sqlglot.exp.Where)

        if where_clause:
            # Existing WHERE clause - combine with AND
            combined = sqlglot.exp.And(this=where_clause.this, expression=rls_condition)
            where_clause.replace(sqlglot.exp.Where(this=combined))
        else:
            # No WHERE clause - create new one
            parsed.set("where", sqlglot.exp.Where(this=rls_condition))

        return parsed.sql(dialect="mysql")

    except Exception as e:
        logger.error(f"{RED}✗ Error adding RLS filter: {e}{RESET}")
        return query


def execute_in_starrocks(query: str) -> list:
    """
    Execute query in StarRocks.

    Args:
        query: SQL query to execute

    Returns:
        List of result rows (as dictionaries)
    """
    try:
        conn = pymysql.connect(**STARROCKS_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        cursor.execute(query)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        logger.info(f"{GREEN}✓ Executed query successfully{RESET}")
        logger.info(f"{CYAN}  Returned {len(rows)} rows{RESET}")

        return rows

    except Exception as e:
        logger.error(f"{RED}✗ Error executing query in StarRocks: {e}{RESET}")
        return []


def format_query_for_display(query: str) -> str:
    """Format query for pretty printing."""
    # Use sqlglot to format the query nicely
    try:
        parsed = sqlglot.parse_one(query, read="mysql")
        return parsed.sql(dialect="mysql", pretty=True)
    except Exception:
        return query


def main():
    """Main execution."""
    logger.info(f"{BLUE}{'='*70}")
    logger.info("RLS Query Filter - Dynamic WHERE Clause Injection")
    logger.info(f"{'='*70}{RESET}")

    # Get email from user
    email = input(f"\n{CYAN}Enter email address: {RESET}").strip()

    if not email:
        logger.error(f"{RED}✗ Email cannot be empty{RESET}")
        return

    logger.info(f"{BLUE}Looking up territories for: {email}{RESET}")
    territories = get_territories_by_email(email)

    if not territories:
        logger.error(f"{RED}✗ No territories found for this email{RESET}")
        return

    logger.info(f"{GREEN}✓ Found {len(territories)} territories:{RESET}")
    logger.info(f"  {territories[:10]}{'...' if len(territories) > 10 else ''}")

    # Get query from user
    logger.info(f"\n{CYAN}Paste your SQL query (press Enter twice when done):{RESET}")
    lines = []
    empty_count = 0
    while True:
        line = input()
        if line:
            lines.append(line)
            empty_count = 0
        else:
            empty_count += 1
            if empty_count >= 2:
                break

    original_query = "\n".join(lines).strip()

    if not original_query:
        logger.error(f"{RED}✗ Query cannot be empty{RESET}")
        return

    logger.info(f"\n{YELLOW}Original Query:{RESET}")
    logger.info(f"\n{format_query_for_display(original_query)}\n")

    # Add RLS filter
    logger.info(f"{BLUE}Adding RLS filter...{RESET}")
    filtered_query = add_rls_filter(original_query, territories)

    logger.info(f"\n{YELLOW}Filtered Query (with RLS):{RESET}")
    logger.info(f"\n{format_query_for_display(filtered_query)}\n")

    # Ask to execute
    confirm = input(f"{CYAN}Execute query in StarRocks? (y/n): {RESET}").strip().lower()

    if confirm == "y":
        logger.info(f"{BLUE}Executing query...{RESET}")
        rows = execute_in_starrocks(filtered_query)

        if rows:
            logger.info(f"\n{GREEN}Query Results ({len(rows)} rows):{RESET}")

            # Show first few columns of first few rows
            if rows:
                columns = list(rows[0].keys())
                logger.info(f"\n{YELLOW}Columns: {', '.join(columns[:10])}{RESET}")
                logger.info(f"\n{YELLOW}First 3 rows:{RESET}")
                for i, row in enumerate(rows[:3]):
                    logger.info(f"\nRow {i+1}:")
                    for col in columns[:10]:
                        logger.info(f"  {col}: {row[col]}")
        else:
            logger.warning(f"{YELLOW}⚠ No results returned{RESET}")
    else:
        logger.info(f"{YELLOW}Query execution skipped{RESET}")

    logger.info(f"\n{GREEN}{'='*70}")
    logger.info("Done")
    logger.info(f"{'='*70}{RESET}")


if __name__ == "__main__":
    main()
