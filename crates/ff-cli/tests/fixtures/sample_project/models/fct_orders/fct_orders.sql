{{ config(materialized='table', wap='true') }}

SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.customer_tier,
    o.order_date,
    o.amount,
    o.status,
    e.payment_total,
    e.payment_count,
    o.amount - e.payment_total AS balance_due
FROM stg_orders o
LEFT JOIN stg_customers c
    ON o.customer_id = c.customer_id
LEFT JOIN int_orders_enriched e
    ON o.order_id = e.order_id
