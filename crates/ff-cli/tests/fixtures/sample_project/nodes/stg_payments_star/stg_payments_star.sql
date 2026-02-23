{{ config(materialized="view", schema="staging") }} select * from raw_payments
