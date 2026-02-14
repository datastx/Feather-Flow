{{ config(materialized='view') }}

{% from "utils.sql" import cents_to_dollars %}

SELECT
    id,
{% if var("enable_filtering") %}
    {{ cents_to_dollars('amount_cents') }} AS amount_dollars,
{% endif %}
    status
FROM raw_events
