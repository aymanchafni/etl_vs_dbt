-- models/marts/dim_product.sql

SELECT DISTINCT p_partkey AS product_key, 
    p_name AS name, 
    p_mfgr AS manufacturer, 
    p_brand AS brand,      
    p_type AS type,
    p_size AS size,
    p_container AS container
FROM {{ source('tpch_database', 'part') }}