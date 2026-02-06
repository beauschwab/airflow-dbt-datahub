{% macro generate_partition_filter(partition_column='partition_date') %}
    {#
      Helper macro: generates the standard incremental partition filter.
      Used across staging and mart models for consistent partition pruning.

      Usage:
        {% if is_incremental() %}
        where {{ generate_partition_filter() }}
        {% endif %}
    #}
    {{ partition_column }} = '{{ var("partition_date") }}'
{% endmacro %}
