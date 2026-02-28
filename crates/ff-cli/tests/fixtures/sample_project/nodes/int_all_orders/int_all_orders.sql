select
    order_id
    , customer_id
    , order_date
    , order_amount
    , status
    , 'enriched' as source
from int_orders_enriched
where status = 'completed'

union all

select
    order_id
    , customer_id
    , order_date
    ,
    amount as order_amount
    , status
    ,
    'staging' as source
from stg_orders
where status = 'pending'
