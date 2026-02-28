select
    order_id
    , customer_id
    , order_date
    , amount
    , status
from stg_orders
where order_id not in (select order_id from fct_orders_incremental)
