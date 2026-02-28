select
    id as customer_id
    ,
    name as customer_name
    , email
    ,
    created_at as signup_date
    ,
    tier as customer_tier
from raw_customers
