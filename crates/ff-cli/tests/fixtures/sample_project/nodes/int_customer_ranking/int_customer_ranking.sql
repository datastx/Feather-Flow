{{ config(materialized="view", schema="intermediate") }}

select
    c.customer_id,
    c.customer_name,
    m.lifetime_value,
    coalesce(m.lifetime_value, 0) as value_or_zero,
    nullif(m.total_orders, 0) as nonzero_orders
from stg_customers c
inner join int_customer_metrics m on c.customer_id = m.customer_id
