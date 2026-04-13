-- models/marts/dim_customer.sql

SELECT DISTINCT c_custkey AS customer_key, 
    c_name AS name, 
    c_address AS address, 
    c_phone AS phone, 
    c_acctbal AS account_balance,
    c_mktsegment AS market_segment
FROM {{ source('tpch_database', 'customer') }}