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
    dcm.customer_code AS wss_code,
    dcm.customer_group_3 AS wss_group_3,
    UPPER(dcm.customer_name) AS wss_name,
    UPPER(ddm.dealer_city) AS dealer_city,
    ddm.dealer_class AS dealer_class,
    UPPER(ddm.dealer_district) AS dealer_district,
    ddm.dealer_code AS dealer_code,
    ddm.dealer_group_code AS dealer_group_code,
    ddm.dealer_name AS dealer_name,
    UPPER(ddm.dealer_route_name) AS dealer_route_name,
    UPPER(ddm.dealer_state) AS dealer_state,
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
    dmm_concat.final_classification AS final_classification,
    UPPER(dmm.brand) AS brand,
    UPPER(dmm_concat.vertical) AS vertical,
    fis.invoice_date AS invoice_date,
    fis.invoice_no AS invoice_no,
    dmm.parent_division_name AS parent_division_name,
    UPPER(dcm.pop_strata) AS pop_strata,
    dm.material_description AS product_description,
    dm.sales_group_code AS product_division_code,
    dsg.vertical AS sub_division,
    UPPER(dm.product_group_1_description) AS product_category,
    UPPER(dm.product_group_2_description) AS product_sub_category,
    UPPER(dm.product_group_3_description) AS product_name,
    dm.product_group_5_description AS pack_size,
    fis.reporting_unit_in_each AS quantity,
    fis.record_type AS record_type,
    fis.reporting_value AS sales,
    fis.revised_net_value_mvg AS uvg,
    dsg.vertical AS sales_group,
    fis.sales_group_code AS sales_group_code,
    UPPER(dsg.vertical) AS sales_group_name,
    dcm.cluster_code AS sh_3_code,
    UPPER(dcm.cluster) AS sh_3_name,
    dcm.branch_code AS sh_4_code,
    dcm.branch AS sh_4_name,
    dcm.cm_region_code AS sh_5_code,
    UPPER(dcm.region) AS sh_5_name,
    dcm.zone_code AS sh_6_code,
    UPPER(dcm.zone) AS sh_6_name,
    ddm.tsi_territory_code AS tsi_code,
    UPPER(TRIM(dcm.tsi_territory_name)) AS tsi_name,
    UPPER(TRIM(dh.sh_2_name)) AS tsr_name,
    fis.reporting_unit AS volume,
    dcm.wss_territory_code AS wss_territory_code,
    UPPER(dcm.wss_territory_name) AS wss_territory_name,
    UPPER(dcm.town) AS wss_town,
    fis.dealer_sg_key AS dealer_sg_key,
    ddm.tsi_code AS ssdm_tsr_code,
    ddm.tsi_name AS ssdm_tsr_name
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
    AND fis.invoice_date >= '20230401';""",
    "comments": {
        "view": "Secondary sales view with territory mapping for RLS",
        "columns": {
            "wss_territory_code": "Territory code for RLS filtering",
            "region": "Region information from territory mapping",
        },
    },
}
