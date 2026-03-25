{{ config(materialized='table', schema='gold') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY code) AS product_sk,
    code,
    product_name,
    brands,
    quantity,
    categories,
    nutriscore_grade,
    nova_group,
    additives_n,
    ingredients_n,
    scans_n,
    unique_scans_n,
    created_at,
    last_modified_at
FROM {{ ref('silver_products') }}
WHERE code IS NOT NULL
