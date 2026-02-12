/*
  Singular test: mart_positions aggregation outlier detection
  ----------------------------------------------------------
  Fails (or warns, per severity) when any rule flags an outlier
  for the current partition_date.

  NOTE: This test reads from mart_positions_dq_metrics, which stores
        both the observed value and the computed thresholds.
*/

select
    partition_date,
    rule_name,
    dims_json,
    current_value,
    hist_n,
    hist_mean,
    hist_stddev,
    lower_threshold,
    upper_threshold,
    zscore,
    stddev_multiplier,
    lookback_business_days,
    min_history_days,
    metric_expr,
    filter_sql,
    group_by_cols
from {{ ref('mart_positions_dq_metrics') }}
where partition_date = cast('{{ var("partition_date") }}' as date)
  and is_outlier = true
