{{ config(materialized='view') }}

SELECT
    id,
    event_type,
{% if var("tier") == "gold" %}
    'premium' AS tier_label,
{% elif var("tier") == "silver" %}
    'standard' AS tier_label,
{% else %}
    'basic' AS tier_label,
{% endif %}
    status
FROM raw_events
