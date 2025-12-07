"""
Configuration for RLS system
Hardcoded credentials for PostgreSQL (source of RLS rules)
"""

# PostgreSQL Configuration (stores RLS rules)
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "datawiz",
    "user": "datawiz_admin",
    "password": "0jqhC3X541tP1RmR.5",
}

# StarRocks Configuration (target database)
STARROCKS_CONFIG = {
    "host": "localhost",
    "port": 9030,
    "user": "datawiz_admin",
    "password": "0jqhC3X541tP1RmR.5",  # Empty by default
}

# RLS Rules table name in PostgreSQL
RLS_RULES_TABLE = "RlsMaster"

# Table: stores rules like:
# user_id | permission | table_name | where_condition
# 'user1' | 'select'   | 'orders'   | 'region = "ASIA"'
# 'user1' | 'select'   | 'customers' | 'sales_team = "Team A"'
