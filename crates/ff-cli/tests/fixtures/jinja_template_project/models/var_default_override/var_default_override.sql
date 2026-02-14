{{ config(materialized='view') }}

SELECT
    id,
    event_type,
    status
FROM raw_events
WHERE event_type = '{{ var("event_category", "default_category") }}'
