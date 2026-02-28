SELECT
    id,
    status
FROM raw_events
WHERE status = '{{ var("known_var") }}'
