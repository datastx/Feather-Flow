{{ config(materialized='view', schema='staging') }}

SELECT
    id AS payment_id,
    order_id,
    CAST(amount AS DECIMAL(10,2)) AS amount
FROM raw_payments
