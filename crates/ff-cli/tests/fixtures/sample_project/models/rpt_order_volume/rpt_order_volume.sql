{{ config(materialized='table', schema='reports') }}

SELECT
    status,
    order_count,
    safe_divide(order_count, 100) AS pct_of_hundred
FROM order_volume_by_status({{ var("min_order_count") }})
