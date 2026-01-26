-- utils.sql - Common utility macros

{% macro cents_to_dollars(column_name) %}
({{ column_name }} / 100.0)
{% endmacro %}

{% macro safe_divide(numerator, denominator, default=0) %}
CASE
  WHEN {{ denominator }} = 0 THEN {{ default }}
  ELSE {{ numerator }} / {{ denominator }}
END
{% endmacro %}

{% macro date_trunc_to_day(date_column) %}
DATE_TRUNC('day', {{ date_column }})
{% endmacro %}
