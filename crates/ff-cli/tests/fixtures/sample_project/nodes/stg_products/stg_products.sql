select
    id as product_id
    ,
    name as product_name
    , category
    , cast(
        price as decimal(
            10
            , 2
        )
    ) as price
    , active
from raw_products
