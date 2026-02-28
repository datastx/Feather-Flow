select
    id
    ,
    {{ hash("user_id") }} as user_hash
    , event_type
    , event_date
    ,
    {{ coalesce_columns(["amount_dollars", "status"]) }} as first_non_null
from stg_events
