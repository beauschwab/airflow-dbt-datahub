# Local development

## Start the stack

```bash
cp .env.example .env
docker compose up -d
```

Airflow UI: http://localhost:8080 (admin/admin)

## dbt commands

From repo root:

```bash
cd dbt/liquidity_analytics

dbt deps --profiles-dir ..
dbt build --profiles-dir .. --vars '{"partition_date": "2025-01-01", "iceberg_catalog": "nessie"}'
```

## Serve docs

```bash
pip install -r requirements-docs.txt
python -m mkdocs serve
```
