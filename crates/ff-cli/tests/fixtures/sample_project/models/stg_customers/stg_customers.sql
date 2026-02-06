{{ config(materialized='view', schema='staging') }}

SELECT
    id AS customer_id,
    name AS customer_name,
    email,
    created_at AS signup_date,
    tier AS customer_tier
FROM raw_customers
