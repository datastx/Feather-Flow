{{ config(materialized='table') }}

-- Fact table joining users and events
SELECT
    e.event_id,
    e.user_id,
    u.email AS user_email,
    u.signup_date,
    e.event_type,
    e.event_timestamp,
    e.properties
FROM stg_events e
LEFT JOIN stg_users u
    ON e.user_id = u.user_id
