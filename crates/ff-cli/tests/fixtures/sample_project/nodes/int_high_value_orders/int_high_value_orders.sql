select
    o.customer_id
    ,
    count(o.order_id) as order_count
    ,
    sum(o.amount) as total_amount
    ,
    min(o.amount) as min_order
    ,
    max(o.amount) as max_order
    ,
    avg(o.amount) as avg_order
from stg_orders o
group by o.customer_id
having sum(o.amount) > 100
