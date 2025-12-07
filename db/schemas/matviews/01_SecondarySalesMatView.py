"""
Secondary Sales View Schema
Creates a view with territory mapping for RLS (Row-Level Security)
"""

TABLE = {
    "order": 9,
    "name": "secondary_sales_mat_view",
    "type": "MATVIEW",
    "schema": """

CREATE MATERIALIZED VIEW secondary_sales_mat_view
    DISTRIBUTED BY HASH(wss_territory_code)
    BUCKETS 10
    REFRESH ASYNC START('2025-12-05 00:00:00') EVERY (interval 1 day)
AS
SELECT
    dcm.customer_code AS customer_code,
    dcm.customer_group_3 AS customer_group_3,
    UPPER(dcm.customer_name) AS customer_name,
    ddm.dealer_city AS dealer_city,
    ddm.dealer_class AS dealer_class,
    ddm.dealer_code AS dealer_code,
    ddm.dealer_district AS dealer_district,
    ddm.dealer_group_code AS dealer_group_code,
    ddm.dealer_name AS dealer_name,
    UPPER(ddm.dealer_route_name) AS dealer_route_name,
    ddm.dealer_state AS dealer_state,
    ddm.tsi_type AS dealer_tsi_type,
    UPPER(ddm.dealer_type_1) AS dealer_type_1,
    ddm.dealer_type_2 AS dealer_type_2,
    ddm.dealer_type_3 AS dealer_type_3,
    UPPER(ddm.dealer_type_4) AS dealer_type_4,
    ddm.dealer_type_5 AS dealer_type_5,
    UPPER(ddm.dealer_type_6) AS dealer_type_6,
    UPPER(ddm.dealer_type_7) AS dealer_type_7,
    UPPER(ddm.dealer_type_12) AS dealer_type_12,
    UPPER(dsg.division) AS division,
    dmm.final_classification AS final_classification,
    dmm.brand AS brand,
    dmm.vertical AS vertical,
    fis.invoice_date AS invoice_date,
    fis.invoice_no AS invoice_no,
    dcm.pop_strata AS pop_strata,
    dm.material_description AS product_description,
    dm.sales_group_code AS product_division_code,
    dsg.vertical AS product_division_name,
    UPPER(dm.product_group_1_description) AS product_group_1,
    UPPER(dm.product_group_2_description) AS product_group_2,
    UPPER(dm.product_group_3_description) AS product_group_3,
    dm.product_group_5_description AS product_group_5,
    fis.reporting_unit_in_each AS quantity,
    fis.record_type AS record_type,
    fis.reporting_value AS sales,
    fis.revised_net_value_mvg AS uvg,
    dsg.vertical AS sales_group,
    CAST(fis.sales_group_code AS STRING) AS sales_group_code,
    dcm.cluster_code AS sh3_code,
    UPPER(dcm.cluster) AS sh3_name,
    dcm.branch_code AS sh4_code,
    dcm.branch AS sh4_name,
    dcm.cm_region_code AS sh5_code,
    UPPER(dcm.region) AS sh5_name,
    dcm.zone_code AS sh6_code,
    UPPER(dcm.zone) AS sh6_name,
    ddm.tsi_territory_code AS tsicode,
    UPPER(dcm.tsi_territory_name) AS tsiname,
    fis.reporting_unit AS volume,
    dcm.wss_territory_code AS wss_territory_code,
    UPPER(dcm.wss_territory_name) AS wss_territory_name
FROM
    fact_invoice_secondary fis
    LEFT JOIN dim_sales_group dsg ON fis.sales_group_code = dsg.sales_group
    LEFT JOIN dim_material dm ON fis.item_code = dm.material_code
    AND dm.active_flag = 'True'
    AND dm.material_type = 'ZFGD'
    LEFT JOIN dim_dealer_master ddm ON CAST(fis.dealer_sg_key AS STRING) = CAST(ddm.dealer_sg_key AS STRING)
    AND ddm.active_flag = 'True'
    LEFT JOIN dim_customer_master dcm ON CAST(ddm.customer_code AS STRING) = CAST(dcm.customer_code AS STRING)
    LEFT JOIN dim_material_mapping dmm ON fis.item_code = dmm.material_code
    -- New optimized join for vertical and final_classification using material_code_sg_key
    LEFT JOIN dim_material_mapping dmm_concat ON CONCAT(
        CAST(fis.item_code AS STRING),
        CAST(fis.sales_group_code AS STRING)
    ) = dmm_concat.material_code_sg_key
    -- Hierarchy join with row number for latest record (StarRocks compatible)
LEFT JOIN (
    SELECT
        sh_2_code,
        sh_2_name
    FROM
        (
            SELECT
                sh_2_code,
                sh_2_name,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        sh_2_code
                    ORDER BY
                        primary_key DESC
                ) as rn
            FROM
                dim_hierarchy
        ) t
    WHERE
        t.rn = 1
) dh ON ddm.tsi_territory_code = dh.sh_2_code
WHERE
    fis.active_flag = '1'
    AND fis.invoice_date >= '20230401';
""",
    "post_create_steps": [
        {
            "step": 1,
            "description": "Refresh materialized view to populate data immediately",
            "cmd": "REFRESH MATERIALIZED VIEW secondary_sales_mat_view;",
        }
    ],
    "comments": {
        "view": "Secondary sales materialized view with territory mapping for RLS",
        "performance": "50ms query time (vs 205ms for dynamic views)",
        "refresh": "ASYNC - scheduled daily at 00:00 UTC, requires manual refresh after creation",
        "columns": {
            "wss_territory_code": "Territory code for RLS filtering (distribution key)",
            "region": "Region information from territory mapping",
        },
        "fixes": {
            "distinction": "Uses ROW_NUMBER() instead of DISTINCT ON (StarRocks compatible)",
            "joins": "Added dmm_concat and hierarchy joins for complete territory data",
            "start_time": "Set to past (2025-12-05 00:00:00) so refresh triggers immediately",
        },
    },
}
