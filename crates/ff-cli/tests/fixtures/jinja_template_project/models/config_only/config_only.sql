{{ config(materialized='table', schema='staging') }}

SELECT
    id,
    event_type,
    status
FROM raw_events
