select
    o.amount
    , coalesce(
        c.name
        , 'Unknown'
    ) as name
    , coalesce(
        c.email
        , 'none@example.com'
    ) as email
from stg_orders o
left join stg_customers c on o.customer_id = c.id
