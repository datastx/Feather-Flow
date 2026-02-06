{{ config(materialized='view', schema='staging') }}

SELECT
    id AS order_id,
    user_id AS customer_id,
    created_at AS order_date,
    amount,
    status
FROM analytics.raw_orders
WHERE created_at >= '{{ var("start_date") }}'
