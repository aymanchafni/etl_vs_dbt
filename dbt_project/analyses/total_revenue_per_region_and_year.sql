-- Region qui a généré le plus de revenus par année

SELECT 
    r.region_name, 
    d.year, 
    COUNT(DISTINCT f.order_key) AS total_orders,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_region') }} r ON f.region_key = r.region_key
JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
GROUP BY r.region_name, d.year
ORDER BY d.year DESC, total_revenue DESC