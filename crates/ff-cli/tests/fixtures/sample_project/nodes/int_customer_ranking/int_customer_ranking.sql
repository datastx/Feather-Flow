{{ config(materialized='view', schema='intermediate') }}

SELECT
    c.customer_id,
    c.customer_name,
    m.lifetime_value,
    COALESCE(m.lifetime_value, 0) AS value_or_zero,
    NULLIF(m.total_orders, 0) AS nonzero_orders
FROM stg_customers c
INNER JOIN int_customer_metrics m
    ON c.customer_id = m.customer_id
