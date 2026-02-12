# Architecture

This project runs a daily, partitioned pipeline with four steps:

1. Spark ingestion (3 parallel tasks)
2. dbt transform (Cosmos `DbtTaskGroup`, renders the full dbt DAG)
3. DQ publish to DataHub (supplemental to OpenLineage facets)
4. Iceberg maintenance (OPTIMIZE + VACUUM)

```mermaid
flowchart TB
  subgraph Airflow[Airflow 3.1 DAG]
    A[ingest_market_data_spark] --> D[dbt_transform (Cosmos factory)]
    B[ingest_positions_loans_spark] --> D
    C[ingest_positions_deposits_spark] --> D
    D --> E[publish_dq_to_datahub]
    E --> F[iceberg_maintenance]
  end

  D --> OL[OpenLineage events]
  A --> OL
  B --> OL
  C --> OL
  F --> OL

  OL --> DH[DataHub GMS]
  E --> DH
```

See the Airflow implementation in
https://github.com/beauschwab/airflow-dbt-datahub/blob/main/dags/partitioned_dbt_spark_iceberg_dag.py.
