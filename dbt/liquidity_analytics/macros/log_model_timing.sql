{% macro log_model_timing() %}
    {#
      Macro to inject timing metadata into model output.
      Useful for monitoring dbt model execution in DataHub.
    #}
    current_timestamp() as _dbt_loaded_at,
    '{{ invocation_id }}' as _dbt_invocation_id
{% endmacro %}
