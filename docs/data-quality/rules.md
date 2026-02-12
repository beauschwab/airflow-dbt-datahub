# Rule authoring

Rule definitions for `mart_positions` live in the macro:

- https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dbt/liquidity_analytics/macros/mart_positions_dq_rules.sql

The macro `mart_positions_dq_rule_defs()` returns a list of rule objects.

## Rule fields

Each rule supports:

- `name` (required)
- `metric_expr` (required): Spark SQL aggregation expression, e.g. `sum(notional_value)`
- `group_by` (optional): list of dimension columns to group by
- `where` (optional): SQL filter predicate
- `lookback_business_days` (optional, default 60)
- `stddev_multiplier` (optional, default 3)
- `min_history_days` (optional, default 20)

## Example

A rule that compares `sum(notional_value)` by `source_system` using a 60-business-day baseline and a 3-sigma threshold:

```yaml
{
  "name": "mart_positions__notional__by_source_system",
  "metric_expr": "sum(notional_value)",
  "group_by": ["source_system"],
  "where": "notional_value is not null",
  "stddev_multiplier": 3,
  "lookback_business_days": 60,
  "min_history_days": 20
}
```

## Implementation details

The generic compiler macro is:

- https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dbt/liquidity_analytics/macros/dq_agg_stddev_thresholds.sql

Business days are inferred from `partition_date` values where `dayofweek(partition_date)` is Mon–Fri.
