{{ config(materialized='table', wap='true') }}

SELECT
    e.order_id,
    e.customer_id,
    c.customer_name,
    c.customer_tier,
    e.order_date,
    e.order_amount AS amount,
    e.status,
    e.payment_total,
    e.payment_count,
    e.order_amount - e.payment_total AS balance_due
FROM int_orders_enriched e
INNER JOIN stg_customers c
    ON e.customer_id = c.customer_id
