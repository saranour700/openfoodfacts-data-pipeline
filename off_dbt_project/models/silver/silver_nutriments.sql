{{ config(materialized='table') }}

WITH source AS (
    SELECT
        code,
        data
    FROM {{ source('bronze', 'raw_products') }}
),

nutriments_extracted AS (
    SELECT
        code,
        CAST(data->>'nutriments.energy-kcal_100g' AS FLOAT) as energy_kcal,
        CAST(data->>'nutriments.sugars_100g' AS FLOAT) as sugars_g,
        CAST(data->>'nutriments.fat_100g' AS FLOAT) as fat_g,
        CAST(data->>'nutriments.proteins_100g' AS FLOAT) as proteins_g,
        CAST(data->>'nutriments.carbohydrates_100g' AS FLOAT) as carbohydrates_g,
        CAST(data->>'nutriments.sodium_100g' AS FLOAT) as sodium_g,
        CAST(data->>'nutriments.fiber_100g' AS FLOAT) as fiber_g
    FROM source
    WHERE code IS NOT NULL
)

SELECT
    code,
    energy_kcal,
    sugars_g,
    fat_g,
    proteins_g,
    carbohydrates_g,
    sodium_g,
    fiber_g
FROM nutriments_extracted
