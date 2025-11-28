"""
RlsMaster Table Definition
Stores Row-Level Security (RLS) hierarchy paths for access control
"""

TABLE = {
    "order": 9,
    "name": "RlsMaster",
    "schema": """CREATE TABLE RlsMaster (
                Cluster VARCHAR(200),
                Division VARCHAR(50),
                EmailID VARCHAR(200),
                HierarchyPath VARCHAR(2000),
                SalesGroup SMALLINT,
                SH2 VARCHAR(200),
                SH3 VARCHAR(200),
                SH4 VARCHAR(200),
                SH5 VARCHAR(200),
                SH6 VARCHAR(200),
                SH7 VARCHAR(200),
                Vertical VARCHAR(50)
            )
            DISTRIBUTED BY HASH(EmailID) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1"
            );""",
    "comments": {
        "table": "Stores Row-Level Security (RLS) hierarchy paths for access control.",
        "columns": {},
    },
    "indexes": {},
}
