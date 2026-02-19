{{ config(materialized='view', schema='staging') }}

SELECT * FROM raw_payments
