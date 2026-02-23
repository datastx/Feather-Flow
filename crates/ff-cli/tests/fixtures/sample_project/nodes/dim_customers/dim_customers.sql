{{
    config(
        materialized="table",
        schema="analytics",
        wap="true",
        post_hook="INSERT INTO hook_log (model, hook_type) VALUES ('dim_customers', 'post')",
    )
}}

select
    m.customer_id,
    c.customer_name,
    c.email,
    c.signup_date,
    m.total_orders,
    m.lifetime_value,
    m.last_order_date,
    case
        when m.lifetime_value >= 1000
        then 'platinum'
        when m.lifetime_value >= 500
        then 'gold'
        when m.lifetime_value >= 100
        then 'silver'
        else 'bronze'
    end as computed_tier
from int_customer_metrics m
inner join stg_customers c on m.customer_id = c.customer_id
