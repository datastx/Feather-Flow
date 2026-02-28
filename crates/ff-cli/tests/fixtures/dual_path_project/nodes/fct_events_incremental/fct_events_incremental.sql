select
    event_id
    , event_type
    , created_at
    , amount
from stg_events
{% if is_exists() %}
    where created_at > (select max(created_at) from fct_events_incremental)
{% endif %}
