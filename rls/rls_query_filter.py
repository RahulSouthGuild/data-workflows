#!/usr/bin/env python3
"""
RLS Query Filter - Dynamic WHERE clause injection using sqlglot
1. Takes a query (with wss_territory_code column)
2. Asks for email
3. Looks up territory codes from rls_map
4. Finds all source tables in the query
5. Uses sqlglot to add WHERE clause filtering at optimal locations
6. Executes query in StarRocks
7. Shows before/after query
"""

import sqlglot
import pymysql
import psycopg2
import sys
from pathlib import Path
import logging
from typing import Dict

# Add project root to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from rls.rls_source_config import (
    get_rls_column,
    get_priority,
    is_rls_enabled_table,
)
from rls.config import PG_CONFIG, STARROCKS_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ANSI colors
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
RED = "\033[31m"
CYAN = "\033[36m"
RESET = "\033[0m"


# ============================================================================
# NEW: SOURCE TABLE DETECTION AND RLS INJECTION FUNCTIONS
# ============================================================================


def find_source_tables(query: str) -> Dict[str, Dict]:
    """
    Find all source tables referenced in the query that have RLS enabled.

    Args:
        query: SQL query to analyze

    Returns:
        Dictionary of {table_name: {info, locations}}
        where locations tells us where the table appears (in CTE, JOIN, etc)
    """
    try:
        # Use postgres dialect to properly handle double-quoted identifiers
        parsed = sqlglot.parse_one(query, read="postgres")
        source_tables = {}

        # Find all tables in the query
        for table in parsed.find_all(sqlglot.exp.Table):
            table_name = table.name

            # Check if this table has RLS enabled
            if is_rls_enabled_table(table_name):
                if table_name not in source_tables:
                    source_tables[table_name] = {
                        "rls_column": get_rls_column(table_name),
                        "priority": get_priority(table_name),
                        "locations": [],
                    }

                # Record where this table appears
                parent = table.parent
                location_type = "unknown"

                # Determine context (CTE, JOIN, FROM, etc)
                if isinstance(parent, sqlglot.exp.From):
                    location_type = "from_clause"
                elif isinstance(parent, sqlglot.exp.Join):
                    location_type = "join_clause"
                elif isinstance(parent, sqlglot.exp.CTE):
                    location_type = "cte"

                source_tables[table_name]["locations"].append(
                    {
                        "type": location_type,
                        "parent": str(type(parent).__name__),
                    }
                )

        return source_tables

    except Exception as e:
        logger.error(f"{RED}✗ Error finding source tables: {e}{RESET}")
        return {}


def detect_query_type(query: str) -> Dict[str, bool]:
    """
    Detect query structure type to determine optimal RLS injection strategy.

    Args:
        query: SQL query to analyze

    Returns:
        Dictionary indicating query characteristics
    """
    try:
        # Use postgres dialect for parsing
        parsed = sqlglot.parse_one(query, read="postgres")

        return {
            "has_cte": bool(parsed.find(sqlglot.exp.CTE)),
            "has_union": bool(parsed.find(sqlglot.exp.Union)),
            "has_join": bool(parsed.find(sqlglot.exp.Join)),
            "has_subquery": bool(parsed.find(sqlglot.exp.Subquery)),
            "is_simple_select": not bool(parsed.find(sqlglot.exp.Union))
            and not bool(parsed.find(sqlglot.exp.Join))
            and not bool(parsed.find(sqlglot.exp.CTE)),
        }
    except Exception as e:
        logger.error(f"{RED}✗ Error detecting query type: {e}{RESET}")
        return {}


def add_rls_to_table_in_cte(
    parsed_query: sqlglot.exp.Expression,
    table_name: str,
    rls_column: str,
    territories: list,
) -> bool:
    """
    Add RLS filter to a table that appears in a CTE.
    Finds the CTE that contains this table and adds filter there.

    Args:
        parsed_query: Parsed sqlglot expression
        table_name: Name of the table to filter
        rls_column: Column to filter on
        territories: List of territory codes

    Returns:
        True if filter was added, False otherwise
    """
    try:
        # Find the CTE that references this table
        for cte in parsed_query.find_all(sqlglot.exp.CTE):
            # Check if this CTE's expression references the table
            for table in cte.this.find_all(sqlglot.exp.Table):
                if table.name == table_name:
                    # Found the CTE that uses this table
                    # Add WHERE clause to the CTE's SELECT

                    # Create the RLS condition
                    territory_list = ", ".join([f"'{t}'" for t in territories])
                    rls_expr = sqlglot.parse_one(
                        f"{rls_column} IN ({territory_list})",
                        read="mysql",
                        into=sqlglot.exp.Expression,
                    )

                    # Find the SELECT in the CTE
                    select_stmt = cte.this
                    if isinstance(select_stmt, sqlglot.exp.Select):
                        where_clause = select_stmt.args.get("where")

                        if where_clause:
                            # Combine with existing WHERE using AND
                            combined = sqlglot.exp.And(this=where_clause.this, expression=rls_expr)
                            where_clause.set("this", combined)
                        else:
                            # No WHERE clause - create new one
                            select_stmt.set("where", sqlglot.exp.Where(this=rls_expr))

                        logger.info(
                            f"{GREEN}✓ Added RLS filter to CTE containing {table_name}{RESET}"
                        )
                        return True

        return False

    except Exception as e:
        logger.error(f"{RED}✗ Error adding RLS to CTE: {e}{RESET}")
        return False


def add_rls_to_table_in_from(
    parsed_query: sqlglot.exp.Expression,
    table_name: str,
    rls_column: str,
    territories: list,
) -> bool:
    """
    Add RLS filter to a table in FROM clause or simple SELECT.

    Args:
        parsed_query: Parsed sqlglot expression
        table_name: Name of the table to filter
        rls_column: Column to filter on
        territories: List of territory codes

    Returns:
        True if filter was added, False otherwise
    """
    try:
        # For simple SELECTs, add filter to the main WHERE
        if isinstance(parsed_query, sqlglot.exp.Select):
            # Create the RLS condition
            territory_list = ", ".join([f"'{t}'" for t in territories])
            rls_expr = sqlglot.parse_one(
                f"{rls_column} IN ({territory_list})", read="mysql", into=sqlglot.exp.Expression
            )

            where_clause = parsed_query.args.get("where")

            if where_clause:
                # Combine with existing WHERE
                combined = sqlglot.exp.And(this=where_clause.this, expression=rls_expr)
                where_clause.set("this", combined)
            else:
                # Create new WHERE clause
                parsed_query.set("where", sqlglot.exp.Where(this=rls_expr))

            logger.info(f"{GREEN}✓ Added RLS filter to {table_name} in FROM clause{RESET}")
            return True

        return False

    except Exception as e:
        logger.error(f"{RED}✗ Error adding RLS to FROM clause: {e}{RESET}")
        return False


def add_rls_to_union_branches(
    parsed_query: sqlglot.exp.Expression,
    source_tables: Dict[str, Dict],
    territories: list,
) -> bool:
    """
    Handle UNION queries - add RLS filter to each branch separately.

    Args:
        parsed_query: Parsed sqlglot expression
        source_tables: Dictionary of source tables found in query
        territories: List of territory codes

    Returns:
        True if any filters were added
    """
    try:
        filters_added = False

        # Find all SELECT statements in UNION
        for select in parsed_query.find_all(sqlglot.exp.Select):
            # Check if this select references any source table
            for table in select.find_all(sqlglot.exp.Table):
                if table.name in source_tables:
                    rls_col = source_tables[table.name]["rls_column"]
                    territory_list = ", ".join([f"'{t}'" for t in territories])
                    rls_expr = sqlglot.parse_one(
                        f"{rls_col} IN ({territory_list})",
                        read="mysql",
                        into=sqlglot.exp.Expression,
                    )

                    where_clause = select.args.get("where")
                    if where_clause:
                        combined = sqlglot.exp.And(this=where_clause.this, expression=rls_expr)
                        where_clause.set("this", combined)
                    else:
                        select.set("where", sqlglot.exp.Where(this=rls_expr))

                    filters_added = True
                    logger.info(f"{GREEN}✓ Added RLS to {table.name} in UNION branch{RESET}")
                    break  # Only add once per SELECT

        return filters_added

    except Exception as e:
        logger.error(f"{RED}✗ Error adding RLS to UNION: {e}{RESET}")
        return False


def add_rls_smart(
    query: str,
    territories: list,
) -> str:
    """
    Smart RLS injection that finds source tables and adds filters optimally.

    Strategy:
    1. Find all source tables (with RLS enabled)
    2. Detect query type (CTE, UNION, JOIN, etc)
    3. Apply optimal injection:
       - CTE: Filter in the CTE that references source
       - UNION: Filter in each branch
       - JOIN: Filter main table (highest priority)
       - Simple: Filter in WHERE clause

    Args:
        query: Original SQL query
        territories: List of territory codes to filter by

    Returns:
        Modified query with RLS filters at optimal locations
    """
    try:
        logger.info(f"{BLUE}Analyzing query structure...{RESET}")

        # Find source tables
        source_tables = find_source_tables(query)
        if not source_tables:
            logger.warning(f"{YELLOW}⚠ No RLS-enabled source tables found in query{RESET}")
            return query

        logger.info(f"{GREEN}✓ Found {len(source_tables)} RLS-enabled source(s):{RESET}")
        for table_name, info in source_tables.items():
            logger.info(
                f"  • {table_name} → filter on {info['rls_column']} (priority: {info['priority']})"
            )

        # Detect query type
        query_type = detect_query_type(query)
        logger.info(
            f"{BLUE}Query type: {', '.join([k for k, v in query_type.items() if v])}{RESET}"
        )

        # Parse the query using postgres dialect for proper identifier handling
        parsed = sqlglot.parse_one(query, read="postgres")

        # Apply filters based on query type
        filters_applied = 0

        if query_type["has_union"]:
            logger.info(f"{YELLOW}UNION query detected - filtering each branch...{RESET}")
            if add_rls_to_union_branches(parsed, source_tables, territories):
                filters_applied += 1

        elif query_type["has_cte"]:
            logger.info(f"{YELLOW}CTE query detected - filtering in CTEs...{RESET}")
            # Sort by priority (lower = filter first)
            sorted_tables = sorted(source_tables.items(), key=lambda x: x[1]["priority"])

            for table_name, info in sorted_tables:
                if add_rls_to_table_in_cte(parsed, table_name, info["rls_column"], territories):
                    filters_applied += 1

        elif query_type["has_join"]:
            logger.info(f"{YELLOW}JOIN query detected - filtering main table...{RESET}")
            # Filter highest priority table only
            main_table = min(source_tables.items(), key=lambda x: x[1]["priority"])
            table_name, info = main_table
            if add_rls_to_table_in_from(parsed, table_name, info["rls_column"], territories):
                filters_applied += 1

        else:
            logger.info(f"{YELLOW}Simple SELECT detected...{RESET}")
            # Simple query - filter it
            for table_name, info in source_tables.items():
                if add_rls_to_table_in_from(parsed, table_name, info["rls_column"], territories):
                    filters_applied += 1

        if filters_applied == 0:
            logger.warning(f"{YELLOW}⚠ Could not inject RLS filters{RESET}")
            return query

        # Generate SQL with MySQL dialect
        result_query = parsed.sql(dialect="mysql")

        # Normalize the output
        return normalize_quotes(result_query)

    except Exception as e:
        logger.error(f"{RED}✗ Error in smart RLS injection: {e}{RESET}")
        return query


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


def normalize_quotes(query: str) -> str:
    """
    Normalize quote styles in query for StarRocks compatibility.
    - Identifiers should use backticks
    - String literals should use single quotes

    Args:
        query: SQL query with potentially mixed quotes

    Returns:
        Query with normalized quotes suitable for StarRocks
    """
    try:
        # Parse using postgres dialect which respects double quotes as identifiers
        # Then convert to MySQL/StarRocks dialect
        parsed = sqlglot.parse_one(query, read="postgres")

        # Generate with MySQL dialect and proper identifier quoting
        normalized = parsed.sql(
            dialect="mysql",
            identify=False,  # Don't auto-quote, we'll handle it
        )

        # Now convert double quotes to backticks and clean up
        import re

        # Replace quoted identifiers: "name" -> `name`
        normalized = normalized.replace('"', "`")

        # Fix any patterns like alias.'column' to alias.`column`
        normalized = re.sub(r"(\w)\.'(\w+)'", r"\1.`\2`", normalized)

        return normalized

    except Exception as e:
        logger.warning(f"{YELLOW}Could not normalize quotes, using query as-is: {e}{RESET}")
        return query


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
    # email = input(f"\n{CYAN}Enter email address: {RESET}").strip()
    email = "abhishek.bhattacharjee@pidilite.com"  # For testing

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
    # lines = []
    # empty_count = 0
    # while True:
    #     line = input()
    #     if line:
    #         lines.append(line)
    #         empty_count = 0
    #     else:
    #         empty_count += 1
    #         if empty_count >= 2:
    #             break

    # original_query = "\n".join(lines).strip()

    original_query = """WITH
    q3_cy_sales AS (
        SELECT
            tsi_name,
            sh_3_name,
            SUM(sales) as total_sales
        FROM
            secondary_sales_mat_view
        WHERE
            invoice_date >= 20241001
            AND invoice_date <= 20241231
        GROUP BY
            tsi_name,
            sh_3_name
    ),
    q3_ly_sales AS (
        SELECT
            tsi_name,
            sh_3_name,
            SUM(sales) as total_sales
        FROM
            secondary_sales_mat_view
        WHERE
            invoice_date >= 20231001
            AND invoice_date <= 20231231
        GROUP BY
            tsi_name,
            sh_3_name
    ),
    q2_cy_sales AS (
        SELECT
            tsi_name,
            sh_3_name,
            SUM(sales) as total_sales
        FROM
            secondary_sales_mat_view
        WHERE
            invoice_date >= 20240701
            AND invoice_date <= 20240930
        GROUP BY
            tsi_name,
            sh_3_name
    ),
    q2_ly_sales AS (
        SELECT
            tsi_name,
            sh_3_name,
            SUM(sales) as total_sales
        FROM
            secondary_sales_mat_view
        WHERE
            invoice_date >= 20230701
            AND invoice_date <= 20230930
        GROUP BY
            tsi_name,
            sh_3_name
    ),
    growth_calc AS (
        SELECT
            COALESCE(
                q3_cy.tsi_name,
                q3_ly.tsi_name,
                q2_cy.tsi_name,
                q2_ly.tsi_name
            ) as tsi_territory_name,
            COALESCE(
                q3_cy.sh_3_name,
                q3_ly.sh_3_name,
                q2_cy.sh_3_name,
                q2_ly.sh_3_name
            ) as cluster,
            ROUND(
                (
                    COALESCE(q3_cy.total_sales, 0) - COALESCE(q3_ly.total_sales, 0)
                ) * 100.0 / NULLIF(q3_ly.total_sales, 0),
                2
            ) as current_quarter_growth,
            ROUND(
                (
                    COALESCE(q2_cy.total_sales, 0) - COALESCE(q2_ly.total_sales, 0)
                ) * 100.0 / NULLIF(q2_ly.total_sales, 0),
                2
            ) as previous_quarter_growth
        FROM
            q3_cy_sales q3_cy
            LEFT JOIN q3_ly_sales q3_ly ON q3_cy.tsi_name = q3_ly.tsi_name
            AND q3_cy.sh_3_name = q3_ly.sh_3_name
            LEFT JOIN q2_cy_sales q2_cy ON COALESCE(q3_cy.tsi_name, q3_ly.tsi_name) = q2_cy.tsi_name
            AND COALESCE(q3_cy.sh_3_name, q3_ly.sh_3_name) = q2_cy.sh_3_name
            LEFT JOIN q2_ly_sales q2_ly ON COALESCE(q3_cy.tsi_name, q3_ly.tsi_name, q2_cy.tsi_name) = q2_ly.tsi_name
            AND COALESCE(q3_cy.sh_3_name, q3_ly.sh_3_name, q2_cy.sh_3_name) = q2_ly.sh_3_name
        WHERE
            q3_ly.total_sales IS NOT NULL
            AND q2_ly.total_sales IS NOT NULL
    )
SELECT
    tsi_territory_name,
    cluster,
    CONCAT(CAST(current_quarter_growth AS VARCHAR), '%') as current_quarter_growth,
    CONCAT(CAST(previous_quarter_growth AS VARCHAR), '%') as previous_quarter_growth,
    ROUND(
        previous_quarter_growth - current_quarter_growth,
        2
    ) as growth_decline
FROM
    growth_calc
WHERE
    current_quarter_growth < previous_quarter_growth
ORDER BY
    growth_decline DESC;
    """

    if not original_query:
        logger.error(f"{RED}✗ Query cannot be empty{RESET}")
        return

    logger.info(f"\n{YELLOW}Original Query:{RESET}")
    logger.info(f"\n{format_query_for_display(original_query)}\n")

    # Add RLS filter using smart injection
    logger.info(f"{BLUE}Adding RLS filters smartly...{RESET}")
    filtered_query = add_rls_smart(original_query, territories)

    logger.info(f"\n{YELLOW}Filtered Query (with RLS):{RESET}")
    logger.info(f"\n{format_query_for_display(filtered_query)}\n")

    # Ask to execute
    confirm = input(f"{CYAN}Execute query in StarRocks? (y/n): {RESET}").strip().lower()

    if confirm == "y":
        logger.info(f"{BLUE}Executing query...{RESET}")

        # Normalize quotes before executing
        normalized_query = normalize_quotes(filtered_query)
        logger.info(f"{CYAN}Query normalized for StarRocks compatibility{RESET}")

        rows = execute_in_starrocks(normalized_query)

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
