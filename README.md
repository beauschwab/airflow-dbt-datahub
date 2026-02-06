# Partitioned dbt-Spark-Iceberg Pipeline with DataHub Lineage

End-to-end **liquidity analytics pipeline** demonstrating **Airflow 3.1**, **dbt via Astronomer Cosmos (factory pattern)**, and **DataHub metadata/lineage** via OpenLineage — all running on Spark + Iceberg.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                       Airflow 3.1 (KubernetesExecutor)                       │
│                                                                              │
│  STEP 1 — Spark Ingestion  (3 parallel SparkKubernetesOperator tasks)        │
│  ┌───────────────────┐ ┌────────────────────┐ ┌─────────────────────────┐    │
│  │ Ingest Market Data │ │ Ingest Pos (Loans) │ │ Ingest Pos (Deposits)   │    │
│  │  → raw.market_data │ │  → raw.pos_loans   │ │  → raw.pos_deposits     │    │
│  │  outlet: Asset     │ │  outlet: Asset     │ │  outlet: Asset          │    │
│  └────────┬───────────┘ └─────────┬──────────┘ └──────────┬──────────────┘    │
│           └───────────────┬───────┴───────────────────────┘                   │
│                           ▼                                                   │
│  STEP 2 — dbt Transform  (ONE Cosmos DbtTaskGroup — factory pattern)         │
│  ┌──────────────────────────────────────────────────────────────────────┐     │
│  │  Cosmos auto-generates tasks from dbt project:                      │     │
│  │                                                                     │     │
│  │  stg_market_data ─────────────────────────────┐                     │     │
│  │  stg_positions_loans ──┐                      │                     │     │
│  │                        ├──► mart_positions ──┬─┤──► mart_liq_risk    │     │
│  │  stg_positions_deposits┘                    │ └──► mart_cf_forecast  │     │
│  │                                             │                       │     │
│  │  Tests run AFTER_EACH model │ Assets emitted per model              │     │
│  └─────────────────────────────┬───────────────────────────────────────┘     │
│                                ▼                                              │
│  STEP 3 — Publish DQ (@task)                                                 │
│  ┌──────────────────────────────┐                                            │
│  │  Parse run_results.json      │                                            │
│  │  Emit assertions → DataHub   │  (supplemental to OpenLineage auto facets) │
│  │  outlet: dq_report Asset     │                                            │
│  └──────────────┬───────────────┘                                            │
│                 ▼                                                             │
│  STEP 4 — Iceberg Maintenance (SparkKubernetesOperator)                      │
│  ┌──────────────────────────────┐                                            │
│  │  OPTIMIZE + VACUUM all tables│  (6 tables: 3 staging + 3 marts)           │
│  └──────────────────────────────┘                                            │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
                              │
                    OpenLineage Events (3 channels)
                              ▼
                 ┌────────────────────────┐
                 │      DataHub GMS       │
                 │  Dataset Lineage       │   Spark → Iceberg tables
                 │  Job Lineage           │   Airflow DAG → task runs
                 │  DQ Assertions         │   dbt test pass/fail/warn
                 │  Schema Metadata       │   Column-level via OL facets
                 └────────────────────────┘
```

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

**Dual-source positions:** Loans (from LMS) and deposits (from CBS) are staged separately, then unioned at the mart layer with a `source_system` partition (`'LOANS'` | `'DEPOSITS'`).

## Project Structure

```
├── dags/
│   └── partitioned_dbt_spark_iceberg_dag.py    # Airflow 3.1 DAG (Cosmos factory)
│
├── dbt/
│   ├── profiles.yml                            # dbt-spark Thrift profiles (dev/prod)
│   └── liquidity_analytics/                    # dbt project root
│       ├── dbt_project.yml                     # Iceberg defaults, merge incremental
│       ├── packages.yml                        # dbt-utils, dbt-expectations
│       ├── models/
│       │   ├── staging/
│       │   │   ├── _sources.yml                # 3 raw sources + freshness SLAs
│       │   │   ├── _models.yml                 # Staging docs + tests (YAML anchors)
│       │   │   ├── stg_market_data.sql         # Dedup & type-cast market data
│       │   │   ├── stg_positions_loans.sql     # Dedup & type-cast loan positions
│       │   │   └── stg_positions_deposits.sql  # Dedup & type-cast deposit positions
│       │   └── marts/
│       │       ├── _models.yml                 # Mart docs + tests
│       │       ├── mart_positions.sql          # Union loans + deposits (dual partition)
│       │       ├── mart_liquidity_risk.sql     # Liquidity tiers, days-to-liquidate
│       │       └── mart_cashflow_forecast.sql  # 30-day rolling cashflow projection
│       ├── tests/
│       │   ├── generic/
│       │   │   └── test_positive_when_not_null.sql
│       │   └── singular/
│       │       ├── assert_positions_have_both_sources.sql
│       │       ├── assert_risk_records_have_market_data.sql
│       │       └── assert_forecast_has_30_days.sql
│       ├── seeds/
│       │   ├── _seeds.yml
│       │   └── instrument_reference.csv
│       ├── macros/
│       │   ├── generate_partition_filter.sql   # Consistent partition pruning
│       │   ├── cents_to_dollars.sql            # Currency conversion helper
│       │   └── log_model_timing.sql            # on-run-end timing log
│       ├── analyses/
│       └── snapshots/
│
├── config/
│   └── airflow_openlineage.cfg                 # OpenLineage → DataHub transport
│
├── docker-compose.yml                          # Local dev: Airflow + Postgres
├── Dockerfile                                  # Airflow 3.1 + dbt + Cosmos image
├── requirements.txt                            # Python dependencies (pinned)
├── .env.example                                # Secrets template
└── .gitignore
```

## Cosmos Factory Pattern

The DAG uses **one `DbtTaskGroup`** with no `select`/`exclude` — Cosmos parses the entire dbt project and auto-generates:

- **One Airflow task per dbt model** (with correct `ref()` dependencies)
- **One Airflow task per dbt test** (wired `AFTER_EACH` model)
- **One Airflow Asset per model** (`emit_datasets=True`) for data-aware scheduling

**Adding a new dbt model requires zero DAG changes.** Just create a `.sql` file and Cosmos discovers it.

Key `RenderConfig` settings:
```python
RenderConfig(
    load_method=LoadMode.AUTOMATIC,       # manifest → dbt ls → custom parser
    test_behavior=TestBehavior.AFTER_EACH, # test right after each model
    emit_datasets=True,                    # every model → Airflow Asset
)
```

## Three-Layer Dependency Guarantees

| Layer | Mechanism | Scope |
|---|---|---|
| **1. Airflow task graph** | `[ingest_*] >> dbt_transform >> publish_dq >> iceberg_maintenance` | All 3 Spark ingest tasks must complete before any dbt task starts |
| **2. Cosmos `ref()` graph** | `mart_positions.sql` calls `ref('stg_positions_loans')` + `ref('stg_positions_deposits')` | Both staging tasks finish before `mart_positions` — guaranteed by dbt DAG resolution |
| **3. Airflow Assets** | Each ingest task declares `outlets=[Asset(...)]` | Future-proof: if ingestion splits to a separate DAG, use `schedule=(asset_loans & asset_deposits & asset_market)` |

## Lineage Flow to DataHub

Three complementary lineage channels — all automatic:

| Source | Mechanism | What DataHub Sees |
|---|---|---|
| **Airflow tasks** | `apache-airflow-providers-openlineage` | DAG → task hierarchy, run status, duration |
| **Spark jobs** | OpenLineage SparkListener (auto-injected via `spark_inject_transport_info`) | Spark job → Iceberg tables, column-level lineage |
| **dbt models & tests** | Cosmos emits OL datasets + DQ facets | Model lineage, test pass/fail as assertions |

The `publish_dq_to_datahub` task provides **supplemental** rich assertion metadata (categories, row-count metrics, freshness SLA) via the DataHub Python SDK on top of what OpenLineage captures automatically.

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/beauschwab/airflow-dbt-datahub.git
cd airflow-dbt-datahub
cp .env.example .env
# Edit .env with your DataHub token, Spark host, and AWS credentials

# 2. Build and start
docker compose up -d

# 3. Open Airflow UI
open http://localhost:8080    # admin / admin

# 4. Install dbt packages (already done in Docker build, but for local dev):
cd dbt/liquidity_analytics && dbt deps --profiles-dir ../
```

Local stack uses `LocalExecutor` (no K8s/Spark). dbt commands run against whatever Spark Thrift host is in `.env` (`DBT_SPARK_HOST`).

## Package Versions

| Package | Version | Purpose |
|---|---|---|
| `apache-airflow` | 3.1.7 | Orchestration, Asset-aware scheduling, `@task` decorator |
| `apache-airflow-providers-openlineage` | ≥2.10.0 | Auto lineage events → DataHub |
| `apache-airflow-providers-cncf-kubernetes` | ≥10.8.0 | `SparkKubernetesOperator` for Spark-on-K8s |
| `astronomer-cosmos[dbt-spark]` | ≥1.11.0 | dbt orchestration in Airflow (factory pattern) |
| `acryl-datahub-airflow-plugin[airflow3]` | ≥0.14.0 | Enhanced DataHub extractors |
| `dbt-spark[PyHive]` | ≥1.9.0 | dbt adapter for Spark Thrift |
| `dbt-utils` | ≥1.3.0 | Surrogate keys, recency tests |
| `dbt-expectations` | ≥0.10.0 | Great Expectations-style DQ tests |

## Airflow Connections

| Connection ID | Type | Purpose |
|---|---|---|
| `kubernetes_default` | Kubernetes | Cluster for `SparkKubernetesOperator` |
| `spark_thrift` | Spark | Spark Thrift Server (dbt-spark via Cosmos `SparkThriftProfileMapping`) |

## Environment Variables

| Variable | Description |
|---|---|
| `DATAHUB_API_TOKEN` | DataHub GMS API token for lineage emission |
| `DATAHUB_GMS_URL` | DataHub GMS endpoint URL |
| `DBT_SPARK_HOST` | Spark Thrift Server hostname (local dev) |
| `DBT_SPARK_USER` | Spark Thrift user (defaults to `spark`) |
| `AWS_ACCESS_KEY_ID` | AWS credentials for Iceberg/S3 access |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_DEFAULT_REGION` | AWS region |

## Key Design Decisions

1. **Cosmos factory pattern** — One `DbtTaskGroup` renders the entire dbt project. No tag-based splitting, no hardcoded model lists. Cosmos resolves the full `ref()` dependency graph automatically.

2. **Dual-source position architecture** — Loan positions (LMS) and deposit positions (CBS) are ingested, staged, and tested independently, then unioned at the mart layer with a `source_system` partition for efficient pruning.

3. **Three-layer dependency guarantees** — Airflow task graph, Cosmos `ref()` resolution, and Airflow Assets provide defense-in-depth that both staging tables are materialized before `mart_positions`.

4. **Jinja-templated `vars` string** — The `vars` passed to Cosmos must remain a raw Jinja string (not `json.dumps()`) so Airflow renders `{{ data_interval_end | ds }}` at execution time, not parse time.

5. **Assets for cross-DAG scheduling** — Each Spark ingest task declares an `outlet` Asset. Downstream DAGs (or a future split-out dbt DAG) can schedule on `asset_loans & asset_deposits & asset_market` using Airflow 3.x's AND operator.

6. **Split YAML per layer** — `_sources.yml` for raw sources with freshness SLAs, `_models.yml` per folder (staging/marts). YAML anchors (`&stg_positions_columns` / `*stg_positions_columns`) keep the two staging position models DRY.

7. **OpenLineage auto-injection** — `spark_inject_parent_job_info` and `spark_inject_transport_info` in OL config auto-configure Spark jobs with DataHub transport, creating parent-child lineage (Airflow DAG → Spark job → Iceberg table).

8. **Iceberg maintenance** — OPTIMIZE (compaction) and VACUUM (snapshot cleanup) run at the end across all 6 tables (3 staging + 3 marts).
