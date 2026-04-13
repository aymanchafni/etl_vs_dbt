-- Top 5 des clients qui ont généré le plus de revenus

SELECT 
    c.name AS customer_name, 
    COUNT(DISTINCT f.order_key) AS total_orders,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_customer') }} c ON f.customer_key = c.customer_key
GROUP BY c.name
ORDER BY total_revenue DESC