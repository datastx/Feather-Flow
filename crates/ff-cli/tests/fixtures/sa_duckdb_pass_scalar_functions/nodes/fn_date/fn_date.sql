select
    date_trunc(
        'month'
        , ts
    ) as trunc_ts
    , date_part(
        'year'
        , ts
    ) as yr
from raw_data
