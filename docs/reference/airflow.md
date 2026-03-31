# Airflow DAG reference

The pipeline is orchestrated by:

- https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dags/partitioned_dbt_spark_iceberg_dag.py

## Key behaviors

- 3 parallel ingestion tasks (`SparkKubernetesOperator`)
- one Cosmos `DbtTaskGroup` renders the entire dbt project graph (no `select`/`exclude`)
- tests run `AFTER_EACH` model
- DQ publish task enriches DataHub with assertion payloads (including aggregation thresholds)
- optional `trigger_qualytics_scan` task calls the Qualytics REST API for Spark/Iceberg containers after dbt completes

## Connections

- `spark_thrift` (dbt + DQ metrics query)
- `kubernetes_default` (Spark on K8s)

## Variables

- `datahub_gms_url`: DataHub REST endpoint (defaults to `http://datahub-gms:8080`)
- `qualytics_api_url`: Qualytics API base URL
- `qualytics_api_token`: Qualytics bearer token
- `qualytics_datastore_name`: Qualytics datastore to scan
- `qualytics_container_names`: comma-separated container names; leave empty to scan the whole datastore
- `qualytics_incremental_scan`: whether to request an incremental scan (`true` by default)
- `qualytics_request_timeout_seconds`: timeout for each Qualytics API request
- `qualytics_poll_interval_seconds`: polling interval while waiting for scan completion
- `qualytics_poll_timeout_seconds`: overall timeout while waiting for the scan to finish
