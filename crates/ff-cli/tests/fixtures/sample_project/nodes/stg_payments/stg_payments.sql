{{ config(materialized="view", schema="staging") }}

select id as payment_id, order_id, {{ cents_to_dollars("amount") }} as amount
from raw_payments
