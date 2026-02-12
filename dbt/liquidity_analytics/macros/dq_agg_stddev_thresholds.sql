{#
  dq_agg_stddev_thresholds.sql
  ---------------------------
  Generic helpers + rule compiler for "compare current aggregation to trailing
  N business days" using standard deviation thresholds.

  Designed for SparkSQL / dbt-spark.
#}

{% macro dq__cols_csv(cols) -%}
  {%- if cols is string -%}
    {{ cols }}
  {%- elif cols is sequence and (cols | length) > 0 -%}
    {{ cols | join(', ') }}
  {%- else -%}
    {{ return('') }}
  {%- endif -%}
{%- endmacro %}

{% macro dq__dims_json_expr(cols) -%}
  {%- if cols is sequence and (cols | length) > 0 -%}
    to_json(named_struct(
      {%- for c in cols -%}
        '{{ c }}', {{ c }}{% if not loop.last %}, {% endif %}
      {%- endfor -%}
    ))
  {%- else -%}
    to_json(named_struct())
  {%- endif -%}
{%- endmacro %}

{% macro dq__business_day_filter(date_expr) -%}
  -- Spark: dayofweek() returns 1=Sunday .. 7=Saturday
  dayofweek({{ date_expr }}) between 2 and 6
{%- endmacro %}

{#
  Returns a SELECT statement (string) that produces one row per group (dims)
  for a single rule, including current value + trailing-window thresholds.

  rule keys:
    - name (required)
    - metric_expr (required) e.g. "sum(notional_value)"
    - group_by (optional list[str])
    - where (optional SQL string)
    - stddev_multiplier (optional, default=3)
    - lookback_business_days (optional, default=60)
    - min_history_days (optional, default=20)

  Notes:
    - "business days" are inferred from the model's own available partition dates.
    - When history is insufficient (< min_history_days), rule will not flag outliers.
#}
{% macro dq__agg_stddev_rule_sql(model_ref, rule, as_of_partition_date, dates_cte_name='dq_dates') -%}
  {%- set rule_name = rule['name'] -%}
  {%- set metric_expr = rule['metric_expr'] -%}
  {%- set group_by = rule.get('group_by', []) -%}
  {%- set where_sql = rule.get('where', '1=1') -%}
  {%- set k = rule.get('stddev_multiplier', 3) -%}
  {%- set lookback = rule.get('lookback_business_days', 60) -%}
  {%- set min_hist = rule.get('min_history_days', 20) -%}

  select
      '{{ rule_name }}' as rule_name,
      {{ dq__dims_json_expr(group_by) }} as dims_json,
      c.partition_date,
      c.current_value,
      b.hist_n,
      b.hist_mean,
      b.hist_stddev,
      (b.hist_mean - ({{ k }} * b.hist_stddev)) as lower_threshold,
      (b.hist_mean + ({{ k }} * b.hist_stddev)) as upper_threshold,
      case
          when b.hist_stddev is null or b.hist_stddev = 0 then null
          else (c.current_value - b.hist_mean) / b.hist_stddev
      end as zscore,
      cast({{ k }} as double) as stddev_multiplier,
      cast({{ lookback }} as int) as lookback_business_days,
      cast({{ min_hist }} as int) as min_history_days,
      '{{ metric_expr }}' as metric_expr,
      '{{ where_sql | replace("'", "''") }}' as filter_sql,
      '{{ (group_by | join(",")) if (group_by is sequence and (group_by | length) > 0) else "" }}' as group_by_cols,
      case
          when b.hist_n is null or b.hist_n < {{ min_hist }} then false
          when b.hist_stddev is null then false
          when b.hist_stddev = 0 then (c.current_value <> b.hist_mean)
          else (abs((c.current_value - b.hist_mean) / b.hist_stddev) > {{ k }})
      end as is_outlier,
      current_timestamp() as computed_at
  from (
      select
          cast('{{ as_of_partition_date }}' as date) as partition_date
          {%- if group_by is sequence and (group_by | length) > 0 %},
          {{ dq__cols_csv(group_by) }}
          {%- endif %},
          {{ metric_expr }} as current_value
      from {{ model_ref }}
      where cast(partition_date as date) = cast('{{ as_of_partition_date }}' as date)
        and ({{ where_sql }})
      group by
          {%- if group_by is sequence and (group_by | length) > 0 -%}
          {{ dq__cols_csv(group_by) }}
          {%- else -%}
          cast('{{ as_of_partition_date }}' as date)
          {%- endif %}
  ) c
  left join (
      select
          {%- if group_by is sequence and (group_by | length) > 0 -%}
          {{ dq__cols_csv(group_by) }}
          {%- else -%}
          1 as _dq_singleton
          {%- endif %},
          count(*) as hist_n,
          avg(metric_value) as hist_mean,
          stddev_samp(metric_value) as hist_stddev
      from (
          select
              cast(partition_date as date) as partition_date
              {%- if group_by is sequence and (group_by | length) > 0 %},
              {{ dq__cols_csv(group_by) }}
              {%- endif %},
              {{ metric_expr }} as metric_value
          from {{ model_ref }}
          where cast(partition_date as date) in (
              select partition_date
              from {{ dates_cte_name }}
              where rnk <= {{ lookback }}
          )
            and ({{ where_sql }})
          group by
              cast(partition_date as date)
              {%- if group_by is sequence and (group_by | length) > 0 %},
              {{ dq__cols_csv(group_by) }}
              {%- endif %}
      ) dq_hist_daily
      group by
          {%- if group_by is sequence and (group_by | length) > 0 -%}
          {{ dq__cols_csv(group_by) }}
          {%- else -%}
          1
          {%- endif %}
  ) b
    on (
      {%- if group_by is sequence and (group_by | length) > 0 -%}
        {%- for c in group_by -%}
          c.{{ c }} <=> b.{{ c }}{% if not loop.last %} and {% endif %}
        {%- endfor -%}
      {%- else -%}
        1 = 1
      {%- endif -%}
    )
{%- endmacro %}
