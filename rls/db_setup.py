w"""
Database setup for RLS system
Creates necessary tables in PostgreSQL
"""

import psycopg2
from psycopg2 import sql
from config import PG_CONFIG


class DatabaseSetup:
    """Setup and manage RLS database tables"""

    def __init__(self, config: dict = None):
        self.config = config or PG_CONFIG
        self.connection = None

    def connect(self):
        """Connect to PostgreSQL"""
        try:
            self.connection = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
            )
            self.cursor = self.connection.cursor()
            print(f"✅ Connected to PostgreSQL")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise

    def disconnect(self):
        """Close connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("✅ Disconnected from PostgreSQL")

    def create_user_access_map_table(self):
        """
        Create UserAccessMap table to store email -> dealers mapping

        Table structure:
        - email: user email
        - dealers: comma-separated dealer codes
        - created_at: timestamp
        - updated_at: timestamp
        """
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS "UserAccessMap" (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) NOT NULL UNIQUE,
                dealers TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_user_email ON "UserAccessMap"(email);
            """

            self.cursor.execute(create_table_sql)
            self.connection.commit()
            print("✅ UserAccessMap table created/verified")
        except Exception as e:
            print(f"❌ Failed to create UserAccessMap table: {e}")
            raise

    def clear_user_access_map(self):
        """Clear all records from UserAccessMap for fresh sync"""
        try:
            self.cursor.execute('TRUNCATE TABLE "UserAccessMap";')
            self.connection.commit()
            print("✅ UserAccessMap cleared")
        except Exception as e:
            print(f"❌ Failed to clear UserAccessMap: {e}")
            raise

    def check_rls_master_exists(self):
        """Check if RlsMaster table exists with ltree"""
        try:
            self.cursor.execute(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'RlsMaster'
                );
            """
            )
            exists = self.cursor.fetchone()[0]

            if exists:
                # Check if it has ltree column
                self.cursor.execute(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'RlsMaster'
                """
                )
                columns = [row[0] for row in self.cursor.fetchall()]
                print(f"✅ RlsMaster exists with columns: {columns}")
                return True
            else:
                print("❌ RlsMaster table does not exist")
                return False
        except Exception as e:
            print(f"❌ Error checking RlsMaster: {e}")
            return False

    def check_dim_customer_master_exists(self):
        """Check if DimCustomerMaster table exists"""
        try:
            self.cursor.execute(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'DimCustomerMaster'
                );
            """
            )
            exists = self.cursor.fetchone()[0]
            print(f"{'✅' if exists else '❌'} DimCustomerMaster exists: {exists}")
            return exists
        except Exception as e:
            print(f"❌ Error checking DimCustomerMaster: {e}")
            return False

    def check_dim_dealer_master_exists(self):
        """Check if DimDealerMaster table exists"""
        try:
            self.cursor.execute(
                """
                SELECT EXISTS(
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = 'DimDealerMaster'
                );
            """
            )
            exists = self.cursor.fetchone()[0]
            print(f"{'✅' if exists else '❌'} DimDealerMaster exists: {exists}")
            return exists
        except Exception as e:
            print(f"❌ Error checking DimDealerMaster: {e}")
            return False


if __name__ == "__main__":
    db = DatabaseSetup()
    db.connect()

    # Check all necessary tables
    print("\n📋 Checking tables...")
    db.check_rls_master_exists()
    db.check_dim_customer_master_exists()
    db.check_dim_dealer_master_exists()

    # Create UserAccessMap table
    print("\n📝 Setting up UserAccessMap table...")
    db.create_user_access_map_table()

    db.disconnect()
    print("\n✅ Database setup complete!")
