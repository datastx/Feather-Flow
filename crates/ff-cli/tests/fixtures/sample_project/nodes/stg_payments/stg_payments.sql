{{ config(materialized='view', schema='staging') }}

SELECT
    id AS payment_id,
    order_id,
    {{ cents_to_dollars('amount') }} AS amount
FROM raw_payments
