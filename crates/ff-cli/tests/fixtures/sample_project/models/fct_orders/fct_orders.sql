{{ config(materialized='table') }}

SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.customer_tier,
    o.order_date,
    o.amount,
    o.status
FROM stg_orders o
LEFT JOIN stg_customers c
    ON o.customer_id = c.customer_id
