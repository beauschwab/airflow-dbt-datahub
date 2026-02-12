# Data Quality (DQ) overview

This project publishes DQ signals to DataHub via two channels:

1. **dbt tests**: pass/fail/warn captured via OpenLineage facets
2. **Aggregation-based statistical checks**: computed per partition and emitted with both *observed values* and *thresholds* for time-series tracking

## Aggregation threshold checks

For `mart_positions`, we compute rule-driven aggregations for the current `partition_date` and compare them to a trailing baseline:

- Baseline window: last **N business days** (default 60)
- Thresholds: $\mu \pm k\sigma$
- Flag: outlier if $|z| > k$ where $z = \frac{x - \mu}{\sigma}$

### Where the results live

The model https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dbt/liquidity_analytics/models/marts/mart_positions_dq_metrics.sql
materializes one row per:

- `partition_date`
- `rule_name`
- `dims_json` (dimension values serialized as JSON)

…and includes:

- `current_value`
- `hist_mean`, `hist_stddev`, `hist_n`
- `lower_threshold`, `upper_threshold`
- `zscore`
- `is_outlier`

### Failing the run

The singular test
https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dbt/liquidity_analytics/tests/singular/assert_mart_positions_agg_outliers_within_threshold.sql
fails (or warns, depending on severity) when any rule produces an outlier for the run partition.
