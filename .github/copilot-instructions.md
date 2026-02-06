# Copilot Instructions — airflow-dbt-datahub

## Architecture Overview

This is a **liquidity analytics pipeline** with four steps orchestrated by a single Airflow 3.1 DAG:

1. **Spark Ingestion** — Three parallel `SparkKubernetesOperator` tasks load raw data into Iceberg staging tables (market data, loan positions, deposit positions)
2. **dbt Transform** — One Cosmos `DbtTaskGroup` (factory pattern) renders the entire dbt project graph (3 staging models → 3 mart models, with tests `AFTER_EACH`)
3. **DQ Publish** — `@task` parses `run_results.json` and emits assertion metadata to DataHub via SDK (supplements OpenLineage auto-facets)
4. **Iceberg Maintenance** — OPTIMIZE + VACUUM on all 6 tables (3 staging + 3 marts)

All lineage flows automatically to **DataHub** via OpenLineage (Airflow provider + Spark listener + Cosmos facets).

## Key Files

| File | Role |
|---|---|
| `dags/partitioned_dbt_spark_iceberg_dag.py` | Single DAG — Cosmos factory pattern (one `DbtTaskGroup`, no `select`/`exclude`) |
| `dbt/liquidity_analytics/dbt_project.yml` | dbt config — Iceberg defaults, merge incremental, partition by `partition_date` |
| `dbt/profiles.yml` | dbt-spark Thrift profiles (dev/prod); in prod Cosmos uses `SparkThriftProfileMapping` from Airflow connection |
| `config/airflow_openlineage.cfg` | OpenLineage → DataHub transport config |
| `docker-compose.yml` | Local dev: Airflow (LocalExecutor) + Postgres; no Spark/K8s locally |

## dbt Model Graph

```
Sources (Iceberg)              Staging                    Marts
──────────────────           ───────────────            ─────────────────────
raw.market_data      ──►  stg_market_data  ──────────┬─► mart_liquidity_risk
                                                     │
raw.positions_loans  ──►  stg_positions_loans   ──┐  │─► mart_cashflow_forecast
                                                  ├──► mart_positions ──┤
raw.positions_deposits─►  stg_positions_deposits ─┘                    └──►
```

Loan positions (LMS) and deposit positions (CBS) are staged independently, then unioned at the mart layer in `mart_positions` with a dual partition: `partition_date` + `source_system` (`'LOANS'` | `'DEPOSITS'`).

## Cosmos Factory Pattern (Critical)

The DAG uses a **single `DbtTaskGroup`** that auto-generates tasks from the dbt project graph. Adding a new dbt model requires **zero DAG changes**. Do NOT split into multiple `DbtTaskGroup`s by tag — the current design deliberately lets Cosmos resolve the full `ref()` dependency graph:

```
stg_positions_loans   ──┐
                        ├──► mart_positions ──┬──► mart_liquidity_risk
stg_positions_deposits ─┘                    └──► mart_cashflow_forecast
stg_market_data ─────────────────────────────────►
```

Key config in the DAG:
- `LoadMode.AUTOMATIC` — Cosmos picks the best parsing method (manifest → `dbt ls` → custom)
- `TestBehavior.AFTER_EACH` — tests run immediately after their parent model
- `emit_datasets=True` — every dbt model becomes an Airflow `Asset` for data-aware scheduling
- `ExecutionMode.KUBERNETES` — each dbt node runs in its own K8s pod

**Jinja vars**: The `operator_args["vars"]` must remain a raw Jinja-templated string (not `json.dumps()`) so that Airflow renders `{{ data_interval_end | ds }}` at execution time, not at DAG parse time:
```python
"vars": '{"partition_date": "{{ data_interval_end | ds }}", "iceberg_catalog": "nessie"}'
```

## Three-Layer Dependency Guarantees

| Layer | Mechanism | What it ensures |
|---|---|---|
| **Airflow task graph** | `[ingest_*] >> dbt_transform` | All 3 Spark ingest tasks succeed before ANY dbt task |
| **Cosmos `ref()` graph** | `mart_positions.sql` has `ref('stg_positions_loans')` + `ref('stg_positions_deposits')` | Both staging models complete before `mart_positions` |
| **Airflow Assets** | Each ingest task declares `outlets=[Asset(...)]` | If ingestion splits to a separate DAG: `schedule=(asset_loans & asset_deposits & asset_market)` |

No `AssetWatcher` is needed for intra-DAG dependencies — `ref()` resolution is the correct mechanism.

## Airflow Conventions

- **Airflow 3.1 SDK** — use `from airflow.sdk import DAG, Asset, task` (not `airflow.decorators`)
- **Assets, not datasets** — Airflow 3.x renamed Datasets to Assets; use `Asset` class
- **Connections**: `spark_thrift` (dbt), `kubernetes_default` (Spark pods) — no secrets in code
- **`_spark_template()` helper** — builds SparkApplication CRD specs; reuse for any new Spark ingest task
- **OpenLineage auto-inject**: `spark_inject_parent_job_info` and `spark_inject_transport_info` are enabled in config; Spark jobs get lineage transport automatically
- **3 ingest tasks**: `ingest_market_data_spark`, `ingest_positions_loans_spark`, `ingest_positions_deposits_spark` — each emits an `outlet` Asset

## dbt Conventions

- **Adapter**: `dbt-spark` with PyHive (Thrift protocol) — SQL dialect is SparkSQL, not Postgres/DuckDB
- **All models are incremental** using `merge` strategy with Iceberg file format
- **Partition keys**: all models partition on `partition_date`; `mart_positions` also partitions on `source_system`
- **Surrogate keys**: use `dbt_utils.generate_surrogate_key()` — always define a `unique_key` in the model config
- **Dedup pattern in staging**: `row_number() over (partition by <grain> order by _ingestion_ts desc)` then filter `_row_num = 1`
- **YAML per layer**: `_sources.yml` for raw sources, `_models.yml` per folder (staging/marts) — do not merge into one file
- **YAML anchors**: `&stg_positions_columns` / `*stg_positions_columns` keep the two staging position model schemas DRY
- **Tags**: models use `staging` or `marts`; all tests use `data_quality`
- **Test severity**: default `warn` (set in `dbt_project.yml`), override to `error` per-test if needed
- **`store_failures: true`** globally — failed test rows persist to Iceberg for debugging
- **Packages**: `dbt_utils` (surrogate keys, recency) and `dbt_expectations` (range/row-count checks)

### Adding a New dbt Model

1. Create `.sql` file in `models/staging/` or `models/marts/` with the standard config block
2. Add column definitions and tests to the layer's `_models.yml`
3. Use `{{ var("partition_date") }}` for incremental filters, not `_dbt_max_partition`
4. Use the `generate_partition_filter()` macro for consistent partition pruning
5. No DAG changes needed — Cosmos auto-discovers new models

### Adding a New Source System for Positions

1. Add the raw table to `_sources.yml` with freshness SLAs
2. Create `stg_positions_<system>.sql` following the same dedup pattern as `stg_positions_loans.sql`
3. Add model schema to staging `_models.yml` (can reuse `*stg_positions_columns` anchor)
4. Add a `union all` leg in `mart_positions.sql` with the new `source_system` value
5. Update `accepted_values` for `source_system` in marts `_models.yml`
6. Add a `SparkKubernetesOperator` ingest task in the DAG with its own `Asset` outlet
7. Add the new staging + mart tables to the Iceberg maintenance `--tables` list

### Existing Macros

| Macro | Purpose | Usage |
|---|---|---|
| `generate_partition_filter(partition_column)` | Standard incremental partition filter | `where {{ generate_partition_filter() }}` |
| `cents_to_dollars(column)` | Convert cents to dollar decimal | `{{ cents_to_dollars('amount_cents') }}` |
| `log_model_timing()` | on-run-end hook for timing logs | Used in `dbt_project.yml` |

### Existing Tests

| Test | Type | What it checks |
|---|---|---|
| `test_positive_when_not_null` | Generic | Column values > 0 when not null |
| `assert_positions_have_both_sources` | Singular | Both LOANS and DEPOSITS systems present in `mart_positions` for the partition |
| `assert_risk_records_have_market_data` | Singular | Every risk record has a matching market data row |
| `assert_forecast_has_30_days` | Singular | Each portfolio has exactly 30 forecast rows |

## Local Development

```bash
cp .env.example .env              # configure secrets
docker compose up -d              # Airflow webserver at http://localhost:8080 (admin/admin)
cd dbt/liquidity_analytics && dbt deps --profiles-dir ../   # install dbt packages
```

Local stack uses `LocalExecutor` (no K8s/Spark). dbt commands run against whatever Spark Thrift host is in `.env` (`DBT_SPARK_HOST`).

## Testing

- **dbt tests**: defined in `_models.yml` (generic) and `tests/singular/` + `tests/generic/` (custom)
- **Singular tests**: follow `assert_<what_is_checked>` naming convention
- **Generic tests**: follow `test_<what_is_checked>` naming convention
- **dbt-expectations**: use for range/distribution checks (e.g., `expect_column_values_to_be_between`)
- **Source freshness**: `warn_after: 24h`, `error_after: 48h` on all raw sources via `loaded_at_field: _ingestion_ts`
- No Python unit tests; pipeline validation is entirely through dbt tests and Airflow DAG parsing
