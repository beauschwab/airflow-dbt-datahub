# airflow-dbt-datahub

A reference **liquidity analytics pipeline** showing:

- **Airflow 3.1** orchestration
- **Spark on Kubernetes** ingestion + Iceberg maintenance
- **dbt via Astronomer Cosmos** using a single `DbtTaskGroup` *factory pattern*
- **OpenLineage → DataHub** for lineage + schema facets
- **Data quality** via dbt tests *and* aggregation-based statistical thresholds

## Quick start

1. Copy env template:

```bash
cp .env.example .env
```

2. Start local services:

```bash
docker compose up -d
```

3. Airflow UI: http://localhost:8080 (admin/admin)

## Docs

Build and serve this documentation locally:

```bash
pip install -r requirements-docs.txt
python -m mkdocs serve
```
