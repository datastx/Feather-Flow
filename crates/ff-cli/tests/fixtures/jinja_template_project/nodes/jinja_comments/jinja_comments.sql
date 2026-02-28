{# This entire comment block should be stripped from compiled output #}
select
    id
    {# inline comment: selecting event_type next #}
    , event_type
    , status
from raw_events
    {# trailing comment #}
