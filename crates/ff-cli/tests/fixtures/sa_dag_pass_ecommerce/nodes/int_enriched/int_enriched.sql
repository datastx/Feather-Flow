select
    o.id
    , o.amount
    ,
    c.name as customer_name
from stg_orders o
join stg_customers c on o.customer_id = c.id
