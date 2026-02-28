select
    cast(price as integer) as price_int
    ,
    cast(amount as integer) as amount_int
from raw_data
