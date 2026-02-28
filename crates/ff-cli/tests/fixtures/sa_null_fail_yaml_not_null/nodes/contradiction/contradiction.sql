select
    o.id
    , c.name
from stg_orders o
left join stg_customers c on o.customer_id = c.id
