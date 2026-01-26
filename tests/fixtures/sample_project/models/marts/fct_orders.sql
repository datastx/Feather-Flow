{{ config(materialized='table', schema='analytics') }}

SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.customer_tier,
    o.order_date,
    o.amount,
    o.status
FROM staging.stg_orders o
LEFT JOIN staging.stg_customers c
    ON o.customer_id = c.customer_id
