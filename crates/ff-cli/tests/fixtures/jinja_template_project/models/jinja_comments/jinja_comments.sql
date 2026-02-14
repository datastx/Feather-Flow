{{ config(materialized='view') }}

{# This entire comment block should be stripped from compiled output #}

SELECT
    id,
    {# inline comment: selecting event_type next #}
    event_type,
    status
FROM raw_events
{# trailing comment #}
