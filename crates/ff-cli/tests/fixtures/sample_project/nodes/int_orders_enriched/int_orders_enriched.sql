select
    o.order_id
    , o.customer_id
    , o.order_date
    ,
    o.amount as order_amount
    , o.status
    , coalesce(
        sum(p.amount)
        , 0
    ) as payment_total
    ,
    count(p.payment_id) as payment_count
from stg_orders o
inner join stg_payments p on o.order_id = p.order_id
group by
    o.order_id
    , o.customer_id
    , o.order_date
    , o.amount
    , o.status
