{{ config(materialized='table') }}

SELECT
    nutriscore_grade,
    nova_group,
    COUNT(*)             AS product_count,
    AVG(ingredients_n)   AS avg_ingredients,
    AVG(additives_n)     AS avg_additives
FROM {{ ref('silver_products') }}
WHERE nutriscore_grade IS NOT NULL
GROUP BY nutriscore_grade, nova_group
ORDER BY nutriscore_grade, nova_group
