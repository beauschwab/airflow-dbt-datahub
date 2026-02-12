# Airflow DAG reference

The pipeline is orchestrated by:

- https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dags/partitioned_dbt_spark_iceberg_dag.py

## Key behaviors

- 3 parallel ingestion tasks (`SparkKubernetesOperator`)
- one Cosmos `DbtTaskGroup` renders the entire dbt project graph (no `select`/`exclude`)
- tests run `AFTER_EACH` model
- DQ publish task enriches DataHub with assertion payloads (including aggregation thresholds)

## Connections

- `spark_thrift` (dbt + DQ metrics query)
- `kubernetes_default` (Spark on K8s)

## Variables

- `datahub_gms_url`: DataHub REST endpoint (defaults to `http://datahub-gms:8080`)
