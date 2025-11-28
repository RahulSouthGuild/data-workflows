"""
DimSalesGroup Table Definition
Stores unique Sales Group codes and their corresponding divisions and verticals
"""

TABLE = {
    "order": 5,
    "name": "DimSalesGroup",
    "schema": """CREATE TABLE DimSalesGroup (
            Division VARCHAR(50) NOT NULL,
            SalesGroup SMALLINT NOT NULL,
            Vertical VARCHAR(50) NOT NULL
        )
        DISTRIBUTED BY HASH(SalesGroup) BUCKETS 10
        PROPERTIES (
            "replication_num" = "1"
        );""",
    "seed_file": "DimSalesGroup.csv",
    "comments": {
        "table": "Stores unique Sales Group codes and their corresponding divisions and verticals.",
        "columns": {},
    },
    "indexes": {},
}
