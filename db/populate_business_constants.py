"""
Script to populate the business_constants table from SecondarySalesView and PrimarySalesView.
"""

import asyncio
import logging
from typing import Dict, Any
import asyncpg

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration for columns to populate
CONSTANT_COLUMNS = [
    {
        "display_name": "Product Name",
        "group_name": "ProductName",
        "priority": 1,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Zone Name",
        "group_name": "SH5Name",
        "priority": 2,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Cluster Name",
        "group_name": "SH3Name",
        "priority": 3,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "TSI Territory Name",
        "group_name": "TSIName",
        "priority": 4,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Sales Group Name",
        "group_name": "SalesGroupName",
        "priority": 5,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Dealer Type 1",
        "group_name": "DealerType1",
        "priority": 6,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Dealer Type 7  –  PCC Dealer",
        "group_name": "DealerType7",
        "priority": 7,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Dealer Type 6",
        "group_name": "DealerType6",
        "priority": 8,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Dealer Type 4",
        "group_name": "DealerType4",
        "priority": 9,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Dealer Type 12 - Periphery",
        "group_name": "DealerType12",
        "priority": 10,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Product Category",
        "group_name": "ProductCategory",
        "priority": 11,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Product Sub Category",
        "group_name": "ProductSubCategory",
        "priority": 12,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Dealer City",
        "group_name": "DealerCity",
        "priority": 13,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Dealer District",
        "group_name": "DealerDistrict",
        "priority": 14,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Customer Town",
        "group_name": "WSSTown",
        "priority": 15,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Division Name",
        "group_name": "Division",
        "priority": 16,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Sales Vertical",
        "group_name": "Vertical",
        "priority": 17,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Customer Group",
        "group_name": "WSSGroup",
        "priority": 18,
        "view_name": "PrimarySalesView",
    },
    {
        "display_name": "Customer Group 1",
        "group_name": "WSSGroup1",
        "priority": 19,
        "view_name": "PrimarySalesView",
    },
    {
        "display_name": "Customer Group 3",
        "group_name": "WSSGroup3",
        "priority": 20,
        "view_name": "PrimarySalesView",
    },
    {
        "display_name": "Customer Group 3 Status",
        "group_name": "WSSGroup3Status",
        "priority": 21,
        "view_name": "PrimarySalesView",
    },
    {
        "display_name": "Customer Block Status ",
        "group_name": "WSSBlock",
        "priority": 22,
        "view_name": "PrimarySalesView",
    },
    {
        "display_name": "Pop Strata",
        "group_name": "PopStrata",
        "priority": 23,
        "view_name": "PrimarySalesView",
    },
    {
        "display_name": "Dealer State",
        "group_name": "DealerState",
        "priority": 24,
        "view_name": "SecondarySalesView",
    },
    {
        "display_name": "Customer Name",
        "group_name": "WSSName",
        "priority": 25,
        "view_name": "PrimarySalesView",
    },
    {
        "display_name": "WSS Town",
        "group_name": "WSSTown",
        "priority": 26,
    },
    {
        "display_name": "WSS District",
        "group_name": "WSSDistrict",
        "priority": 27,
    },
    {
        "display_name": "WSS State",
        "group_name": "WSSState",
        "priority": 28,
    },
    {
        "display_name": "Brand",
        "group_name": "Brand",
        "priority": 29,
        "view_name": "SecondarySalesView",
    },
]


async def get_connection(
    host: str = "localhost",
    port: int = 5432,
    database: str = "postgres",
    user: str = "postgres",
    password: str = "postgres",
) -> asyncpg.Connection:
    """Create an async PostgreSQL connection."""
    try:
        conn = await asyncpg.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )
        logger.info(f"Connected to PostgreSQL at {host}:{port}/{database}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


async def column_exists_in_view(
    connection: asyncpg.Connection,
    view_name: str,
    column_name: str,
) -> bool:
    """Check if a column exists in a view."""
    try:
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = $1 AND column_name = $2
            )
        """
        result = await connection.fetchval(query, view_name, column_name)
        return result
    except Exception as e:
        logger.warning(f"Error checking column {column_name} in {view_name}: {e}")
        return False


async def populate_business_constants_for_column(
    connection: asyncpg.Connection,
    column_config: Dict[str, Any],
) -> int:
    """
    Populate business_constants table for a specific column.
    Returns the number of rows inserted.
    """
    group_name = column_config["group_name"]
    display_name = column_config["display_name"]
    priority = column_config["priority"]
    view_names = []

    # Collect view names
    if "view_name" in column_config:
        view_names = [column_config["view_name"]]
    else:
        # If no specific view, try both
        view_names = ["SecondarySalesView", "PrimarySalesView"]

    # Check which views have this column
    available_views = []
    for view_name in view_names:
        exists = await column_exists_in_view(connection, view_name, group_name)
        if exists:
            available_views.append(view_name)

    if not available_views:
        logger.warning(f"Column '{group_name}' not found in any view")
        return 0

    # Build the UNION query
    union_parts = []
    for view_name in available_views:
        union_parts.append(
            f"""
            SELECT DISTINCT
                "{group_name}"::text AS value_col,
                "SalesGroupName",
                "SalesGroupCode",
                "WSSTerritoryCode"
            FROM "{view_name}"
            WHERE "Sales" > 10000
                AND "SalesGroupName" IS NOT NULL
                AND "SalesGroupCode" IS NOT NULL
                AND "{group_name}" IS NOT NULL
        """
        )

    union_query = "\nUNION\n".join(union_parts)

    # Full insert query
    insert_query = f"""
        INSERT INTO business_constants (
            group_name,
            display_name,
            display_value,
            value,
            search_text,
            sales_group_codes,
            territory_codes,
            priority,
            is_active,
            created_at,
            updated_at
        )
        SELECT
            $1::text AS group_name,
            $2::text AS display_name,
            combined_data.value_col AS display_value,
            combined_data.value_col AS value,
            LOWER($2::text || ' ' || combined_data.value_col) AS search_text,
            ARRAY_AGG(DISTINCT combined_data."SalesGroupCode") AS sales_group_codes,
            COALESCE(
                ARRAY_AGG(DISTINCT combined_data."WSSTerritoryCode") 
                    FILTER (WHERE combined_data."WSSTerritoryCode" IS NOT NULL),
                ARRAY[]::VARCHAR[]
            ) AS territory_codes,
            $3::integer AS priority,
            TRUE AS is_active,
            NOW() AS created_at,
            NOW() AS updated_at
        FROM (
            {union_query}
        ) combined_data
        GROUP BY combined_data.value_col
        ON CONFLICT DO NOTHING;
    """

    try:
        result = await connection.execute(
            insert_query,
            group_name,
            display_name,
            priority,
        )
        rows_affected = int(result.split()[-1]) if result else 0
        logger.info(f"✓ Populated '{display_name}' (group: {group_name}) - {rows_affected} rows")
        return rows_affected
    except Exception as e:
        logger.error(f"✗ Error populating '{display_name}': {e}")
        return 0


async def main(
    host: str = "localhost",
    port: int = 5432,
    database: str = "postgres",
    user: str = "postgres",
    password: str = "postgres",
):
    """Main function to populate business_constants table."""
    connection = None
    try:
        connection = await get_connection(host, port, database, user, password)

        # Clear existing data (optional - comment out if you want to preserve)
        # await connection.execute("TRUNCATE TABLE business_constants RESTART IDENTITY;")
        # logger.info("Cleared existing business_constants data")

        total_rows = 0
        logger.info(f"\nStarting to populate {len(CONSTANT_COLUMNS)} columns...")
        logger.info("-" * 80)

        for idx, column_config in enumerate(CONSTANT_COLUMNS, 1):
            rows = await populate_business_constants_for_column(connection, column_config)
            total_rows += rows

        logger.info("-" * 80)
        logger.info(f"\n✓ Population complete! Total rows inserted: {total_rows}")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise
    finally:
        if connection:
            await connection.close()
            logger.info("Connection closed")


if __name__ == "__main__":
    # Update these with your actual database credentials
    asyncio.run(
        main(
            host="localhost",
            port=5432,
            database="datawiz",
            user="datawiz_admin",
            password="0jqhC3X541tP1RmR.5",
        )
    )
