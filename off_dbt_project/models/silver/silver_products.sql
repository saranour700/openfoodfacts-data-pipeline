{{ config(materialized='table') }}

WITH source AS (
    SELECT
        _dlt_id,
        code,
        product_name,
        brands,
        quantity,
        categories,
        allergens,
        traces,
        ingredients_text,
        NULLIF(NULLIF(nutriscore_grade, 'unknown'), 'not-applicable') AS nutriscore_grade,
        nova_group,
        additives_n,
        ingredients_n,
        scans_n,
        unique_scans_n,
        to_timestamp(created_t)       AS created_at,
        to_timestamp(last_modified_t) AS last_modified_at,
        countries_tags
    FROM {{ source('bronze', 'products') }}
),

canadian_filter AS (
    SELECT
        *,
        CASE 
            WHEN 'en:canada' = ANY(countries_tags) THEN true
            WHEN countries_tags::VARCHAR ILIKE '%canada%' THEN true
            ELSE false
        END as is_canadian
    FROM source
)

SELECT
    _dlt_id,
    code,
    product_name,
    brands,
    quantity,
    categories,
    allergens,
    traces,
    ingredients_text,
    nutriscore_grade,
    nova_group,
    additives_n,
    ingredients_n,
    scans_n,
    unique_scans_n,
    created_at,
    last_modified_at,
    is_canadian
FROM canadian_filter
WHERE is_canadian = true
  AND product_name IS NOT NULL
  AND code IS NOT NULL
