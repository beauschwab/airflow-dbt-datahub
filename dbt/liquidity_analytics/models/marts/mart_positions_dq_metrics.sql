{#
  mart_positions_dq_metrics.sql
  ----------------------------
  Materializes one row per (partition_date, rule_name, dims_json) containing:
    - current aggregation value for the partition_date
    - trailing-window baseline mean/stddev (last N business dates)
    - thresholds (mean ± k·stddev)
    - outlier flag

  This model exists so we can emit *both* the observed value and thresholds to
  DataHub for time-series tracking, regardless of whether a dbt test fails.
#}

{{
  config(
    materialized='incremental',
    file_format='iceberg',
    unique_key='dq_record_id',
    incremental_strategy='merge',
    partition_by=['partition_date'],
    tags=['marts', 'data_quality']
  )
}}

{% set as_of = var('partition_date') %}

{%- set max_lookback = 0 -%}
{%- for r in mart_positions_dq_rule_defs() -%}
  {%- set lb = r.get('lookback_business_days', 60) -%}
  {%- if lb > max_lookback -%}
    {%- set max_lookback = lb -%}
  {%- endif -%}
{%- endfor -%}
{%- if max_lookback == 0 -%}
  {%- set max_lookback = 60 -%}
{%- endif -%}

with dq_dates as (
  select partition_date, rnk
  from (
    select
      partition_date,
      dense_rank() over (order by partition_date desc) as rnk
    from (
      select distinct cast(partition_date as date) as partition_date
      from {{ ref('mart_positions') }}
      where cast(partition_date as date) < cast('{{ as_of }}' as date)
        and {{ dq__business_day_filter('cast(partition_date as date)') }}
    ) d
  ) ranked
  where rnk <= {{ max_lookback }}
),

dq_rules as (
  {% for rule in mart_positions_dq_rule_defs() %}
    {{ dq__agg_stddev_rule_sql(ref('mart_positions'), rule, as_of, 'dq_dates') }}
    {% if not loop.last %}
    union all
    {% endif %}
  {% endfor %}
)

select
    {{ dbt_utils.generate_surrogate_key([
      'rule_name',
      'dims_json',
      'cast(partition_date as string)'
    ]) }} as dq_record_id,
    *
from dq_rules

{% if is_incremental() %}
where partition_date = cast('{{ as_of }}' as date)
{% endif %}
