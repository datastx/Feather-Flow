select
    id
    , event_type
    , status
from raw_events
where event_type = '{{ var("event_category", "default_category") }}'
