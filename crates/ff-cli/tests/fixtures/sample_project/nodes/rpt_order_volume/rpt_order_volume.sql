select
    status
    , order_count
    , safe_divide(
        order_count
        , 100
    ) as pct_of_hundred
from order_volume_by_status({{ var("min_order_count") }})
