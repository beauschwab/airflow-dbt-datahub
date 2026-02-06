{% macro cents_to_dollars(column_name, precision=2) %}
    round(cast({{ column_name }} as decimal(18,{{ precision }})) / 100, {{ precision }})
{% endmacro %}
