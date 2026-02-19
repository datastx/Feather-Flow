{{ config(materialized='table', schema='analytics') }}

SELECT DISTINCT
    product_id,
    product_name,
    category,
    price,
    CAST(product_id * 10 AS BIGINT) AS id_scaled,
    CASE
        WHEN category = 'electronics' THEN
            CASE
                WHEN price > 100 THEN 'premium_electronics'
                ELSE 'standard_electronics'
            END
        WHEN category = 'tools' THEN 'tools'
        ELSE 'other'
    END AS detailed_category
FROM stg_products
