-- models/staging/stg_orders.sql

SELECT 
    o_orderkey,
    o_custkey,
    o_orderdate,
    o_totalprice
FROM {{ source('tpch_database', 'orders') }}
WHERE o_orderstatus = 'F'