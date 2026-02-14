{{ config(materialized='view') }}

{% from "nonexistent_macros.sql" import some_macro %}

SELECT
    id,
    {{ some_macro('status') }} AS transformed_status
FROM raw_events
