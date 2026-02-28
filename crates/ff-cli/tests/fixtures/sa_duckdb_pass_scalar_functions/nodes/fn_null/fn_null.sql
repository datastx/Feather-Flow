select
    coalesce(
        name
        , 'unknown'
    ) as safe_name
from raw_data
