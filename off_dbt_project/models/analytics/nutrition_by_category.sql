SELECT 
    main_category,
    COUNT(*) as product_count,
    
    ROUND(AVG(energy_kcal_100g), 2) as avg_energy_kcal,
    ROUND(AVG(proteins_100g), 2) as avg_proteins,
    ROUND(AVG(carbohydrates_100g), 2) as avg_carbs,
    ROUND(AVG(fat_100g), 2) as avg_fat,
    ROUND(AVG(sugars_100g), 2) as avg_sugars,
    ROUND(AVG(sodium_100g), 2) as avg_sodium

FROM {{ ref('silver_products') }}
WHERE main_category IS NOT NULL
    AND main_category != ''
GROUP BY main_category
HAVING COUNT(*) >= 5
ORDER BY product_count DESC
