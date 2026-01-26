-- Utility macros for sample project

{% macro cents_to_dollars(column_name) %}
({{ column_name }} / 100.0)
{% endmacro %}

{% macro safe_divide(numerator, denominator, default=0) %}
CASE
  WHEN {{ denominator }} = 0 THEN {{ default }}
  ELSE {{ numerator }} / {{ denominator }}
END
{% endmacro %}
