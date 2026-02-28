select
    e.order_id
    , e.customer_id
    , c.customer_name
    , c.customer_tier
    , e.order_date
    ,
    e.order_amount as amount
    , e.status
    , e.payment_total
    , e.payment_count
    , e.order_amount
    - e.payment_total as balance_due
    , safe_divide(
        e.payment_total
        , e.order_amount
    ) as payment_ratio
from int_orders_enriched e
inner join stg_customers c on e.customer_id = c.customer_id
