{{ config(materialized='view', schema='intermediate') }}

SELECT
    o.customer_id,
    COUNT(o.order_id) AS order_count,
    SUM(o.amount) AS total_amount,
    MIN(o.amount) AS min_order,
    MAX(o.amount) AS max_order,
    AVG(o.amount) AS avg_order
FROM stg_orders o
GROUP BY o.customer_id
HAVING SUM(o.amount) > 100
