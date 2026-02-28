SELECT
    event_id,
    event_type,
    created_at,
    amount
FROM stg_events
{% if is_exists() %}
WHERE created_at > (SELECT MAX(created_at) FROM fct_events_incremental)
{% endif %}
