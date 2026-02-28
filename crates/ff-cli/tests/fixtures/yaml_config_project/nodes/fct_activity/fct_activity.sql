SELECT
    activity_id,
    user_id,
    action,
    ts
FROM raw_activity
{% if is_exists() %}
WHERE ts > (SELECT MAX(ts) FROM fct_activity)
{% endif %}
