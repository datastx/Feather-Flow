select
    o.id
    , o.amount
    , o.status
    ,
    c.name as customer_name
    , o.created_at
    , c.email
from stg_orders o
join stg_customers c on o.customer_id = c.id
