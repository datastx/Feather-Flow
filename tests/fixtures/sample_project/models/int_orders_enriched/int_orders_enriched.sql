{{ config(materialized='view', schema='intermediate') }}

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount AS order_amount,
    o.status,
    COALESCE(SUM(p.amount), 0) AS payment_total,
    COUNT(p.payment_id) AS payment_count
FROM staging.stg_orders o
LEFT JOIN staging.stg_payments p
    ON o.order_id = p.order_id
GROUP BY
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    o.status
