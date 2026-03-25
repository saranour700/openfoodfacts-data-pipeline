SELECT 
    code,
    brands,
    product_name,
    sugars_100g as sugars_g,
    nutriscore_grade,
    main_category
FROM {{ ref('silver_products') }}
WHERE sugars_100g IS NOT NULL
    AND sugars_100g > 0
ORDER BY sugars_100g DESC
LIMIT 20
