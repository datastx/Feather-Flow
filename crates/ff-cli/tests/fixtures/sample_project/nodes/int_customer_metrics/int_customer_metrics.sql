select
    c.customer_id
    , c.customer_name
    ,
    count(o.order_id) as total_orders
    , coalesce(
        sum(o.amount)
        , 0
    ) as lifetime_value
    ,
    max(o.order_date) as last_order_date
from stg_customers c
inner join stg_orders o on c.customer_id = o.customer_id
group by
    c.customer_id
    , c.customer_name
