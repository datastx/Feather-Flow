{{ config(materialized="table", schema="reports") }}

select
    c.customer_id,
    c.customer_name,
    c.email,
    e.order_id,
    e.order_amount,
    e.payment_total,
    (e.order_amount - e.payment_total) * 1.1 as balance_with_fee,
    e.order_amount + e.payment_total + e.payment_count as combined_metric
from stg_customers c
inner join int_orders_enriched e on c.customer_id = e.customer_id
inner join stg_orders o on e.order_id = o.order_id
where e.order_amount between o.amount and o.amount
