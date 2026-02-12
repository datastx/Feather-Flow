{{ config(materialized='view', schema='intermediate') }}

SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.amount), 0) AS lifetime_value,
    MAX(o.order_date) AS last_order_date
FROM stg_customers c
INNER JOIN stg_orders o
    ON c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.customer_name
