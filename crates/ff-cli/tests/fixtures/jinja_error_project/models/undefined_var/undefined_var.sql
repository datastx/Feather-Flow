{{ config(materialized='view') }}

SELECT
    id,
    status
FROM raw_events
WHERE status = '{{ var("nonexistent_variable") }}'
