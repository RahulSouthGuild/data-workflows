"""
Secondary Sales View Schema
Creates a view with territory mapping for RLS (Row-Level Security)
"""

TABLE = {
    "order": 9,
    "name": "secondary_sales_view",
    "type": "VIEW",
    "schema": """CREATE VIEW secondary_sales_view AS
SELECT 
    t1.sale_id,
    t1.customer_id,
    t1.amount,
    t1.invoice_date,
    t1.invoice_no,
    t1.volume,
    t1.volume_in_kg,
    t1.volume_in_ltr,
    t1.value,
    t1.rate,
    t1.dealer_code,
    t1.dealer_sg_key,
    t1.item_code,
    t1.discount_amount,
    t1.cash_discount,
    t1.scheme_amount,
    t1.freight_amount,
    t1.cgst_amount,
    t1.sgst_amount,
    t1.igst_amount,
    t1.vat_amount,
    t1.created_date,
    t1.updated_by_source,
    t2.wss_territory_code,
    t2.region
FROM fact_invoice_secondary t1
LEFT JOIN territory_mapping t2 
    ON t1.customer_id = t2.customer_id
WHERE t2.wss_territory_code IS NOT NULL;""",
    "comments": {
        "view": "Secondary sales view with territory mapping for RLS",
        "columns": {
            "wss_territory_code": "Territory code for RLS filtering",
            "region": "Region information from territory mapping",
        },
    },
}
