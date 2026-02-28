select
    id
    ,
    {% if var("enable_filtering") %}
        {{ cents_to_dollars("amount_cents") }} as amount_dollars
        ,
    {% endif %}
    status
from raw_events
