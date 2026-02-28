SELECT
    id,
    {{ some_macro('status') }} AS transformed_status
FROM raw_events
