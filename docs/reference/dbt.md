# dbt project reference

Project root:

- https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dbt/liquidity_analytics/dbt_project.yml

## Model graph

```mermaid
flowchart LR
  raw_market[raw.market_data] --> stg_market[stg_market_data]
  raw_loans[raw.positions_loans] --> stg_loans[stg_positions_loans]
  raw_deps[raw.positions_deposits] --> stg_deps[stg_positions_deposits]

  stg_market --> risk[mart_liquidity_risk]
  stg_loans --> pos[mart_positions]
  stg_deps --> pos

  pos --> risk
  pos --> cf[mart_cashflow_forecast]
  stg_market --> cf

  pos --> posdq[mart_positions_dq_metrics]
```

## Partitioning

- Most models partition on `partition_date`
- `mart_positions` partitions on both `partition_date` and `source_system`

## Data Quality

- Generic + singular tests live under https://github.com/beauschwab/airflow-dbt-datahub/tree/main/dbt/liquidity_analytics/tests
- Aggregation threshold metrics are materialized in `mart_positions_dq_metrics`
