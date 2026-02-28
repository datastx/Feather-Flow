select
    id
    , user_id
    , event_type
    , status
from raw_events
where status = 'active'
