{{ config(materialized='table') }}

{% from "utils.sql" import format_event_type %}

SELECT
    {{ format_event_type('event_type') }} AS event_type_clean,
    COUNT(*) AS event_count,
    SUM(amount_dollars) AS total_amount,
    {{ safe_divide('amount_dollars', 'id') }} AS amount_per_event
FROM int_events_enriched
GROUP BY event_type
HAVING COUNT(*) >= {{ var("min_event_count") }}
