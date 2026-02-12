# DataHub emission

In addition to OpenLineage’s automatic DQ facets for dbt tests, the Airflow task `publish_dq_to_datahub` emits **enriched assertion run events** that include:

- rule identifiers (`rule_name`, `dims_json`)
- observed value (`current_value`)
- baseline stats (`hist_mean`, `hist_stddev`, `hist_n`)
- thresholds (`lower_threshold`, `upper_threshold`)
- outlier flag (`is_outlier`)

This enables time-series tracking and alerting inside DataHub because each daily run pushes a structured payload for each rule.

## Where it is implemented

- https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dags/partitioned_dbt_spark_iceberg_dag.py

## How it works

1. Parse `target/run_results.json` for dbt test outcomes
2. Query `nessie.marts.mart_positions_dq_metrics` (via Airflow connection `spark_thrift`)
3. Emit one DataHub assertion run event per `(rule_name, dims_hash, partition_date)`

!!! note
    The GMS endpoint is read from the Airflow Variable `datahub_gms_url` (default `http://datahub-gms:8080`).
