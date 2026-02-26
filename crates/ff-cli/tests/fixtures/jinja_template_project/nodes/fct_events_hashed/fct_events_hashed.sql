{{ config(materialized='table') }}

SELECT
    id,
    {{ hash('user_id') }} AS user_hash,
    event_type,
    event_date,
    {{ coalesce_columns(['amount_dollars', 'status']) }} AS first_non_null
FROM stg_events
