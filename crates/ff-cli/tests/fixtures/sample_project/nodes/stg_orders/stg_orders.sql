select
    id as order_id
    , user_id as customer_id
    , created_at as order_date
    , amount
    , status
from raw_orders
where created_at >= '{{ var("start_date") }}'
