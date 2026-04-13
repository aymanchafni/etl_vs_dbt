-- Top 5 des secteurs de marché (market segment) qui ont généré le plus de revenus

SELECT 
    c.market_segment, 
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    SUM(f.quantity) AS total_qty
FROM {{ ref('fact_sales') }} f       
JOIN {{ ref('dim_customer') }} c ON f.customer_key = c.customer_key
GROUP BY c.market_segment
ORDER BY total_revenue DESC