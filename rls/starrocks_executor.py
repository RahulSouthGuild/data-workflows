"""
Execute modified queries on StarRocks
"""

import pymysql
from typing import Dict, List, Optional
from config import STARROCKS_CONFIG


class StarRocksExecutor:
    """Execute queries on StarRocks with RLS"""

    def __init__(self, config: Dict = None):
        """
        Initialize StarRocks connection

        Args:
            config: StarRocks config dict (uses default if None)
        """
        self.config = config or STARROCKS_CONFIG
        self.connection = None

    def connect(self):
        """Connect to StarRocks"""
        try:
            self.connection = pymysql.connect(
                host=self.config["host"],
                port=self.config["port"],
                user=self.config["user"],
                password=self.config["password"],
            )
            print(f"✅ Connected to StarRocks: {self.config['host']}")
        except Exception as e:
            print(f"❌ Failed to connect to StarRocks: {e}")
            raise

    def disconnect(self):
        """Close StarRocks connection"""
        if self.connection:
            self.connection.close()
            print("✅ Disconnected from StarRocks")

    def execute_query(self, query: str, database: str = None) -> List[Dict]:
        """
        Execute query on StarRocks

        Args:
            query: SQL query to execute
            database: Database name (optional)

        Returns:
            List of result rows as dicts
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor(pymysql.cursors.DictCursor)

            # Use database if specified
            if database:
                cursor.execute(f"USE {database}")

            # Execute query
            cursor.execute(query)
            results = cursor.fetchall()

            cursor.close()
            return results

        except Exception as e:
            print(f"❌ Error executing query: {e}")
            return []

    def execute_query_raw(self, query: str, database: str = None) -> List[tuple]:
        """
        Execute query and return raw tuples

        Args:
            query: SQL query to execute
            database: Database name (optional)

        Returns:
            List of result rows as tuples
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()

            # Use database if specified
            if database:
                cursor.execute(f"USE {database}")

            # Execute query
            cursor.execute(query)
            results = cursor.fetchall()

            cursor.close()
            return results

        except Exception as e:
            print(f"❌ Error executing query: {e}")
            return []

    def get_columns(self, database: str, table: str) -> List[str]:
        """
        Get column names for a table

        Args:
            database: Database name
            table: Table name

        Returns:
            List of column names
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(f"USE {database}")
            cursor.execute(f"DESCRIBE {table}")
            columns = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return columns

        except Exception as e:
            print(f"❌ Error getting columns: {e}")
            return []


if __name__ == "__main__":
    # Test executor
    executor = StarRocksExecutor()

    try:
        executor.connect()

        # Test basic query (no database needed)
        print("=== Test 1: Basic connectivity ===")
        results = executor.execute_query("SELECT 1 as test")
        print(f"Results: {results}\n")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        executor.disconnect()
