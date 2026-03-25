WITH brand_counts AS (
    SELECT 
        brands,
        COUNT(*) as product_count
    FROM {{ ref('silver_products') }}  -- silver layer
    WHERE brands IS NOT NULL 
        AND brands != ''
    GROUP BY brands
)

SELECT 
    brands,
    product_count,
    ROUND(
        100.0 * product_count / SUM(product_count) OVER(), 
        2
    ) as percentage_of_total
FROM brand_counts
ORDER BY product_count DESC
LIMIT 20
