{{ config(materialized='view') }}

SELECT
    id,
    user_id,
    event_type,
    {{ date_trunc('month', 'event_date') }} AS event_month,
    amount_dollars,
    status
FROM stg_events
WHERE event_type = '{{ var("event_category") }}'
{% if var("min_event_count") > 0 %}
  AND user_id IS NOT NULL
{% endif %}
