select
    id
    , event_type
    ,
    {% if var("tier") == "gold" %}
        'premium' as tier_label
        ,
    {% elif var("tier") == "silver" %}
        'standard' as tier_label
        ,
    {% else %} 'basic' as tier_label
        ,
    {% endif %}
    status
from raw_events
