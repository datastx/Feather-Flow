{{ config(materialized='view', schema='staging') }}

SELECT
    id AS product_id,
    name AS product_name,
    category,
    CAST(price AS DECIMAL(10,2)) AS price,
    active
FROM analytics.raw_products
