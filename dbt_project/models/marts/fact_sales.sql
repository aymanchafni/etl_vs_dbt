-- models/marts/fact_sales.sql

WITH orders AS (
    SELECT * FROM {{ source('tpch_database', 'orders') }}
),
lineitem AS (
    SELECT * FROM {{ source('tpch_database', 'lineitem') }}
    WHERE l_linestatus = 'F'
)

SELECT  l_orderkey AS order_key,
    l_partkey AS product_key,
    l_shipdate AS date_key,
    o_custkey AS customer_key,
    r_regionkey AS region_key,
    l_quantity AS quantity,
    ROUND(l_extendedprice * (1 - l_discount), 2) AS revenue
FROM lineitem
JOIN orders o ON l_orderkey = o_orderkey
JOIN {{ source('tpch_database', 'customer') }} c ON o_custkey = c_custkey    
JOIN {{ source('tpch_database', 'nation') }} n ON c_nationkey = n_nationkey
JOIN {{ source('tpch_database', 'region') }} r ON n_regionkey = r_regionkey