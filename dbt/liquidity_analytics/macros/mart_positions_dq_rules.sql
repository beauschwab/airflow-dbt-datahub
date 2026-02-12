{#
  mart_positions_dq_rules.sql
  --------------------------
  Central place to define aggregation-based DQ rules for mart_positions.

  Add / remove rules here without touching model SQL.
#}

{% macro mart_positions_dq_rule_defs() %}
  {%- set rules = [
    {
      "name": "mart_positions__row_count__total",
      "metric_expr": "count(*)",
      "group_by": [],
      "where": "1=1",
      "stddev_multiplier": 4,
      "lookback_business_days": 60,
      "min_history_days": 20
    },
    {
      "name": "mart_positions__notional__by_source_system",
      "metric_expr": "sum(notional_value)",
      "group_by": ["source_system"],
      "where": "notional_value is not null",
      "stddev_multiplier": 3,
      "lookback_business_days": 60,
      "min_history_days": 20
    },
    {
      "name": "mart_positions__notional__by_position_type",
      "metric_expr": "sum(notional_value)",
      "group_by": ["position_type"],
      "where": "notional_value is not null",
      "stddev_multiplier": 3,
      "lookback_business_days": 60,
      "min_history_days": 20
    },
    {
      "name": "mart_positions__notional__by_source_and_currency",
      "metric_expr": "sum(notional_value)",
      "group_by": ["source_system", "currency"],
      "where": "currency is not null and notional_value is not null",
      "stddev_multiplier": 3,
      "lookback_business_days": 60,
      "min_history_days": 20
    }
  ] -%}

  {{ return(rules) }}
{% endmacro %}
