{% test positive_when_not_null(model, column_name) %}
{#
  Custom generic test: assert that a column's values are > 0
  when they are not null.

  Usage in schema.yml:
    columns:
      - name: volume
        tests:
          - positive_when_not_null
#}

select
    {{ column_name }} as value_field
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} <= 0

{% endtest %}
