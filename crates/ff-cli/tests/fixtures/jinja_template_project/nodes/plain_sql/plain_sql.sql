SELECT
    id,
    user_id,
    event_type,
    status
FROM raw_events
WHERE status = 'active'
