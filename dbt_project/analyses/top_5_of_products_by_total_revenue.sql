-- Top 5 des produits qui ont généré le plus de revenus

SELECT 
    p.name AS product_name, 
    SUM(f.quantity) AS total_qty,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM {{ ref('fact_sales') }} f
JOIN {{ ref('dim_product') }} p ON f.product_key = p.product_key
GROUP BY p.name
ORDER BY total_revenue DESC