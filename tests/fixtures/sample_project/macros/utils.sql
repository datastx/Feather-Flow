-- utils.sql - Common utility macros for the sample project

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

{% macro date_trunc_to_week(date_column) %}
DATE_TRUNC('week', {{ date_column }})
{% endmacro %}

{% macro date_trunc_to_month(date_column) %}
DATE_TRUNC('month', {{ date_column }})
{% endmacro %}

{% macro generate_surrogate_key(field_list) %}
MD5(CONCAT_WS('|', {% for field in field_list %}{{ field }}{% if not loop.last %}, {% endif %}{% endfor %}))
{% endmacro %}
