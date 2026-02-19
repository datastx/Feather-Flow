{{ config(materialized='table', schema='reports') }}

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    e.order_id,
    e.order_amount,
    e.payment_total,
    (e.order_amount - e.payment_total) * 1.1 AS balance_with_fee,
    e.order_amount + e.payment_total + e.payment_count AS combined_metric
FROM stg_customers c
INNER JOIN int_orders_enriched e
    ON c.customer_id = e.customer_id
INNER JOIN stg_orders o
    ON e.order_id = o.order_id
WHERE e.order_amount BETWEEN o.amount AND o.amount
