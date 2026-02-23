{{
    config(
        materialized="table",
        wap="true",
        pre_hook="CREATE TABLE IF NOT EXISTS hook_log (model VARCHAR, hook_type VARCHAR, ts TIMESTAMP DEFAULT current_timestamp)",
        post_hook=["INSERT INTO hook_log (model, hook_type) VALUES ('fct_orders', 'post')", "INSERT INTO hook_log (model, hook_type) VALUES ('fct_orders', 'post_2')"],
    )
}}

select
    e.order_id,
    e.customer_id,
    c.customer_name,
    c.customer_tier,
    e.order_date,
    e.order_amount as amount,
    e.status,
    e.payment_total,
    e.payment_count,
    e.order_amount - e.payment_total as balance_due,
    safe_divide(e.payment_total, e.order_amount) as payment_ratio
from int_orders_enriched e
inner join stg_customers c on e.customer_id = c.customer_id
