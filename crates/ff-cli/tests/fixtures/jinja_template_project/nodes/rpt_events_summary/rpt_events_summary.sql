select
    {{ format_event_type("event_type") }} as event_type_clean
    ,
    count(*) as event_count
    ,
    sum(amount_dollars) as total_amount
    ,
    {{ safe_divide("amount_dollars", "id") }} as amount_per_event
from int_events_enriched
group by event_type
having count(*) >= {{ var("min_event_count") }}
