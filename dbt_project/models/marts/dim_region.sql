-- models/marts/dim_regions.sql

SELECT DISTINCT r_regionkey AS region_key, r_name AS region_name
FROM {{ source('tpch_database', 'region') }}