{{
    config(
        materialized="incremental",
        unique_key="order_id",
        incremental_strategy="merge"
    )
}}

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    o.status,
    current_timestamp as loaded_at
from stg_orders o
left join fct_orders_incremental existing
    on o.order_id = existing.order_id
where existing.order_id is null
