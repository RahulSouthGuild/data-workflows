"""
Primary Sales View Schema
Creates a view for primary sales data with comprehensive customer, product, and sales dimensions
Security: Uses security_invoker for row-level security (RLS)
"""

TABLE = {
    "order": 10,
    "name": "primary_sales_view",
    "type": "VIEW",
    "schema": """CREATE VIEW primary_sales_view AS
    SELECT
        fid.customer_code AS wss_code,
        UPPER(dcm.customer_group) AS wss_group,
        UPPER(dcm.customer_group_1) AS wss_group_1,
        UPPER(dcm.customer_group_3) AS wss_group_3,
        UPPER(dcm.customer_group_3_status) AS wss_group_3_status,
        UPPER(dcm.customer_name) AS wss_name,
        UPPER(dcm.customer_block) AS wss_block,
        UPPER(dcm.district) AS wss_district,
        dcm.payment_method_name AS payment_method_name,
        dcm.credit_limit AS credit_limit,
        dcm.customer_creation_date AS customer_creation_date,
        dcm.population AS population,
        fid.division_code AS division,
        dmm_concat.final_classification AS final_classification,
        dcm.grand_parent_customer_code AS grand_parent_wss_code,
        fid.invoice_no AS invoice_no,
        fid.invoice_type AS invoice_type,
        fid.posting_date AS invoice_date,
        dcm.parent_customer_code AS parent_wss_code,
        fid.billing_quantity_in_stock_keeping_unit AS quantity,
        UPPER(dcm.pop_strata) AS pop_strata,
        UPPER(dm.product_group_1_description) AS product_category,
        UPPER(dm.product_group_2_description) AS product_sub_category,
        UPPER(dm.product_group_3_description) AS product_name,
        dm.product_group_5_description AS pack_size,
        fid.material_division AS product_division_code,
        fid.mis_type AS mis_type,
        CASE
            WHEN fid.mis_type = 'CN' THEN -1 * fid.net_value
            ELSE fid.net_value
        END AS sales,
        fid.sales_group_code AS sales_group_code,
        UPPER(dmm.brand) AS brand,
        dcm.sale_group_name AS sales_group,
        UPPER(dcm.region) AS sh_5_name,
        dcm.region_code AS sh_5_code,
        UPPER(dmm_concat.vertical) AS vertical,
        dmm.material_code AS material_code,
        dmm.parent_division_code AS parent_division_code,
        dmm.parent_division_name AS parent_division_name,
        dmm.material_code_sg_key AS material_code_sg_key,
        UPPER(dcm.cluster) AS sh_3_name,
        dcm.cluster_code AS sh_3_code,
        dcm.branch AS sh_4_name,
        dcm.branch_code AS sh_4_code,
        UPPER(dcm.zone) AS sh_6_name,
        dcm.zone_code AS sh_6_code,
        UPPER(dsg.vertical) AS sales_group_name,
        UPPER(dcm.state) AS wss_state,
        dcm.term_of_payment AS term_of_payment,
        dcm.tsi_grouping AS tsi_grouping,
        dcm.tsi_territory_code AS tsi_code,
        UPPER(TRIM(dcm.tsi_territory_name)) AS tsi_name,
        fid.reporting_unit AS volume,
        dcm.winomkar_wss_flag AS winomkar_wss_flag,
        dcm.wss_territory_code AS wss_territory_code,
        UPPER(dcm.wss_territory_name) AS wss_territory_name,
        UPPER(dcm.town) AS wss_town
    FROM
        fact_invoice_details fid
        LEFT JOIN dim_customer_master dcm ON fid.customer_code = dcm.customer_code
        LEFT JOIN dim_sales_group dsg ON fid.sales_group_code = dsg.sales_group
        LEFT JOIN dim_material dm ON fid.material_code = dm.material_code
        LEFT JOIN dim_material_mapping dmm ON fid.material_code = dmm.material_code
        -- Optimized join for Vertical and FinalClassification
        LEFT JOIN dim_material_mapping dmm_concat ON CONCAT(fid.material_code, fid.sales_group_code) = dmm_concat.material_code_sg_key
    WHERE
        fid.active_flag = '1'
        AND fid.mis_type IN ('CN', 'INV')""",
}
