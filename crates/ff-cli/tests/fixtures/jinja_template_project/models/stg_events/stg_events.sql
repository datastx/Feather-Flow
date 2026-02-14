{{ config(materialized='view') }}

{% from "utils.sql" import cents_to_dollars %}

SELECT
    id,
    user_id,
    event_type,
    event_date,
    {{ cents_to_dollars('amount_cents') }} AS amount_dollars,
    status
FROM raw_events
WHERE status = '{{ var("default_status", "active") }}'
