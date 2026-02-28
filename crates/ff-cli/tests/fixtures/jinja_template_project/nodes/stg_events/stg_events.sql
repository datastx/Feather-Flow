select
    id
    , user_id
    , event_type
    , event_date
    ,
    {{ cents_to_dollars("amount_cents") }} as amount_dollars
    , status
from raw_events
where status = '{{ var("default_status", "active") }}'
