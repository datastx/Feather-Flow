{{ config(materialized='view') }}

-- Staging model for events
SELECT
    id AS event_id,
    user_id,
    event_type,
    event_timestamp,
    properties
FROM raw_events
WHERE event_timestamp >= '{{ var("start_date") }}'
