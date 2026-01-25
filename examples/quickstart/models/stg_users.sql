{{ config(materialized='view') }}

-- Staging model for users
SELECT
    id AS user_id,
    email,
    created_at AS signup_date,
    status
FROM raw_users
