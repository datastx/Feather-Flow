{% macro cents_to_dollars(column_name) %}({{ column_name }} / 100.0){% endmacro %}

{% macro format_event_type(column_name) %}LOWER(TRIM({{ column_name }})){% endmacro %}
