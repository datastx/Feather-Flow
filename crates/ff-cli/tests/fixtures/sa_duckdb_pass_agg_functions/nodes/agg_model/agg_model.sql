select
    count(*) as cnt
    , sum(amount) as total
    , avg(amount) as avg_amt
    , min(amount) as min_amt
    , max(amount) as max_amt
from raw_data
