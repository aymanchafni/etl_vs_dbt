-- models/marts/dim_date.sql

SELECT DISTINCT l_shipdate AS date_key, 
    YEAR(l_shipdate) AS year, 
    MONTH(l_shipdate) AS month, 
    DAY(l_shipdate) AS day,
    QUARTER(l_shipdate) AS quarter,
    WEEK(l_shipdate) AS week     
FROM {{ source('tpch_database', 'lineitem') }}
-- Same filter as fact_sales (and as the ETL pipeline): only finalised line items
WHERE l_linestatus = 'F'