select
    id
    , user_id
    , event_type
    ,
    {{ date_trunc("month", "event_date") }} as event_month
    , amount_dollars
    , status
from stg_events
where
    event_type = '{{ var("event_category") }}'
    {% if var("min_event_count") > 0 %} and user_id is not null {% endif %}
