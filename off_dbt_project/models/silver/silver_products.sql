{{ config(materialized='table') }}

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
    to_timestamp(last_modified_t) AS last_modified_at
FROM {{ source('bronze', 'products') }}
