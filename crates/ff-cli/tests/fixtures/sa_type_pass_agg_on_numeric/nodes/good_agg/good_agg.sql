select
    sum(amount) as total_amount
    ,
    count(name) as name_count
    ,
    min(name) as first_name
    ,
    max(name) as last_name
from raw_data
