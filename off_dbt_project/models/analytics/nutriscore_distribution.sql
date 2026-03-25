SELECT 
    nutriscore_grade,
    COUNT(*) as product_count,
    ROUND(
        100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 
        2
    ) as percentage
FROM {{ ref('silver_products') }}
WHERE nutriscore_grade IS NOT NULL
    AND nutriscore_grade IN ('a', 'b', 'c', 'd', 'e')
GROUP BY nutriscore_grade
ORDER BY product_count DESC
