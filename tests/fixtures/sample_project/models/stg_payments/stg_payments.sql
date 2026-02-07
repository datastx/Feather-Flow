{{ config(materialized='view', schema='staging') }}

SELECT
    id AS payment_id,
    order_id,
    payment_method,
    CAST(amount AS DECIMAL(10,2)) AS amount,
    created_at AS payment_date
FROM analytics.raw_payments
