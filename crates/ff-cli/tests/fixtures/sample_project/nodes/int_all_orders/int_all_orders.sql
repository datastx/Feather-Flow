{{ config(materialized='view', schema='intermediate') }}

SELECT
    order_id,
    customer_id,
    order_date,
    order_amount,
    status,
    'enriched' AS source
FROM int_orders_enriched
WHERE status = 'completed'

UNION ALL

SELECT
    order_id,
    customer_id,
    order_date,
    amount AS order_amount,
    status,
    'staging' AS source
FROM stg_orders
WHERE status = 'pending'
