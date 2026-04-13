-- Revenue total par année et par trimestre

SELECT 
    d.year,
    d.quarter,
    COUNT(DISTINCT f.order_key) AS total_orders,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
GROUP BY d.year, d.quarter
ORDER BY d.year DESC, d.quarter DESC