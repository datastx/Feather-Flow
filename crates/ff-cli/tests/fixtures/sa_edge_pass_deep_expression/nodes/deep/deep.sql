select
    cast(
        coalesce(
            case when amount > 0 then amount else 0 end
            , 0
        ) as bigint
    ) as safe_amount
from raw_data
