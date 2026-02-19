{{ config(materialized='table', schema='analytics') }}

SELECT
    product_id,
    product_name,
    category,
    price,
    CASE
        WHEN category = 'electronics' THEN 'high_value'
        WHEN category = 'tools' THEN 'medium_value'
        ELSE 'standard'
    END AS category_group,
    CASE
        WHEN price > 100 THEN 'premium'
        WHEN price > 25 THEN 'standard'
        ELSE 'budget'
    END AS price_tier
FROM stg_products
WHERE active = true
