{{ config(materialized='table', schema='analytics') }}

SELECT
    m.customer_id,
    c.customer_name,
    c.email,
    c.signup_date,
    m.total_orders,
    m.lifetime_value,
    m.last_order_date,
    CASE
        WHEN m.lifetime_value >= 1000 THEN 'platinum'
        WHEN m.lifetime_value >= 500 THEN 'gold'
        WHEN m.lifetime_value >= 100 THEN 'silver'
        ELSE 'bronze'
    END AS computed_tier
FROM int_customer_metrics m
INNER JOIN stg_customers c
    ON m.customer_id = c.customer_id
