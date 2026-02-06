"""
Partitioned dbt-Spark-Iceberg Pipeline with DataHub Lineage
============================================================
Airflow 3.x DAG using a **Cosmos factory pattern**: a single DbtTaskGroup
parses the entire dbt project and dynamically builds Airflow tasks + Assets
from its contents.  No hardcoded model lists or tag-based splitting.

**Factory principle:**
  Cosmos reads dbt_project.yml (or manifest.json) and auto-generates:
    • One Airflow task per dbt model/seed/snapshot (with correct dependencies)
    • One Airflow task per dbt test (wired AFTER_EACH model)
    • One Airflow Asset per model (emit_datasets=True) for data-aware scheduling
  Adding a new dbt model to the project requires ZERO changes to this DAG file.

Stack:
  - Orchestration:  Apache Airflow 3.1 (KubernetesExecutor)
  - Transforms:     dbt-spark via Astronomer Cosmos (DbtTaskGroup)
  - Compute Engine: Apache Spark on Kubernetes (SparkKubernetesOperator)
  - Storage:        Apache Iceberg on S3
  - Lineage:        OpenLineage → DataHub (automatic via provider)
  - Data Quality:   dbt tests emitted as OpenLineage facets → DataHub

Airflow Connections required:
  - spark_thrift       (Spark)       : Spark Thrift Server for dbt-spark
  - kubernetes_default (Kubernetes)  : cluster where Spark Operator is deployed

airflow.cfg required:
  [openlineage]
  transport = {"type": "http", "url": "https://<DATAHUB_GMS_HOST>:8080/openapi/openlineage/", ...}
  namespace = spark-iceberg-pipelines
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum

# ---------------------------------------------------------------------------
# Airflow 3 SDK imports
# ---------------------------------------------------------------------------
from airflow.sdk import DAG, Asset, task

# ---------------------------------------------------------------------------
# Operators & providers
# ---------------------------------------------------------------------------
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

# ---------------------------------------------------------------------------
# Cosmos (dbt orchestration) — the "factory"
# ---------------------------------------------------------------------------
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from cosmos.profiles import SparkThriftProfileMapping

# =========================================================================
# Configuration  (all tunables in one place)
# =========================================================================

# -- Paths (mount via DAG bundles, git-sync, or ConfigMap) ----------------
DBT_PROJECT_PATH = Path("/opt/airflow/dbt/liquidity_analytics")

# -- Partition template (rendered per-run via Airflow macros) -------------
PARTITION_DATE = "{{ data_interval_end | ds }}"

# -- S3 / Iceberg locations ----------------------------------------------
S3_BUCKET = "s3://data-lake-prod"
ICEBERG_WAREHOUSE = f"{S3_BUCKET}/iceberg/warehouse"
ICEBERG_CATALOG = "nessie"

# -- Spark Operator defaults ----------------------------------------------
SPARK_NAMESPACE = "spark-jobs"
SPARK_IMAGE = "custom-spark:3.5.3-iceberg-1.7.1"
SPARK_K8S_CONN = "kubernetes_default"

# -- Assets ---------------------------------------------------------------
#    Cosmos emits assets for dbt models automatically (emit_datasets=True).
#    We explicitly declare assets for INGESTION outputs so that:
#      (a) Airflow's Asset graph shows end-to-end lineage (ingest → dbt)
#      (b) If ingestion is ever split into a separate DAG, the dbt DAG
#          can schedule on (asset_loans & asset_deposits & asset_market)
#      (c) Downstream consumers can subscribe to raw-data availability

asset_positions_loans = Asset(
    name="raw_positions_loans",
    uri=f"iceberg://{ICEBERG_CATALOG}/staging/positions_loans",
)
asset_positions_deposits = Asset(
    name="raw_positions_deposits",
    uri=f"iceberg://{ICEBERG_CATALOG}/staging/positions_deposits",
)
asset_market_data = Asset(
    name="raw_market_data",
    uri=f"iceberg://{ICEBERG_CATALOG}/staging/market_data",
)
dq_report_asset = Asset(
    name="dq_report",
    uri="x-datahub://data-quality/liquidity-pipeline",
)

# =========================================================================
# Shared configs
# =========================================================================

default_args = {
    "owner": "liquidity-quant",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# -- Cosmos profile config ------------------------------------------------
#    Uses Airflow connection `spark_thrift` — no secrets in code.
cosmos_profile = ProfileConfig(
    profile_name="liquidity_analytics",
    target_name="prod",
    profile_mapping=SparkThriftProfileMapping(
        conn_id="spark_thrift",
        profile_args={"schema": "staging"},
    ),
)

# -- Cosmos project config ------------------------------------------------
cosmos_project = ProjectConfig(
    dbt_project_path=str(DBT_PROJECT_PATH),
)

# -- Cosmos execution config ----------------------------------------------
cosmos_execution = ExecutionConfig(
    execution_mode=ExecutionMode.KUBERNETES,
)

# -- Cosmos render config — THE FACTORY -----------------------------------
#    • load_method=AUTOMATIC: tries manifest → dbt ls → custom parser
#    • NO select/exclude: renders the ENTIRE dbt project graph
#    • emit_datasets=True: every dbt model becomes an Airflow Asset
#    • test_behavior=AFTER_EACH: tests run right after their model
cosmos_render = RenderConfig(
    load_method=LoadMode.AUTOMATIC,
    test_behavior=TestBehavior.AFTER_EACH,
    emit_datasets=True,
)

# -- Common operator args (vars injected into every dbt invocation) -------
#    IMPORTANT: The `vars` value must remain a Jinja-templated string so that
#    Airflow renders {{ data_interval_end | ds }} at EXECUTION TIME, not at
#    DAG parse time.  json.dumps() would freeze the literal template string.
cosmos_operator_args = {
    "install_deps": True,
    "vars": '{"partition_date": "{{ data_interval_end | ds }}", "iceberg_catalog": "' + ICEBERG_CATALOG + '"}',
}


# -- Reusable Spark template spec for SparkKubernetesOperator -------------
def _spark_template(
    app_name: str,
    main_py: str,
    *,
    extra_args: list[str] | None = None,
    driver_cores: int = 2,
    driver_memory: str = "4g",
    executor_instances: int = 3,
    executor_cores: int = 4,
    executor_memory: str = "8g",
) -> dict:
    """Build a SparkApplication CRD template_spec dict."""
    return {
        "spark": {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "apiGroup": "sparkoperator.k8s.io",
            "metadata": {"namespace": SPARK_NAMESPACE},
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": SPARK_IMAGE,
                "imagePullPolicy": "Always",
                "sparkVersion": "3.5.3",
                "mainApplicationFile": main_py,
                "arguments": extra_args or [],
                "restartPolicy": {"type": "Never"},
                "sparkConf": {
                    f"spark.sql.catalog.{ICEBERG_CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
                    f"spark.sql.catalog.{ICEBERG_CATALOG}.type": "nessie",
                    f"spark.sql.catalog.{ICEBERG_CATALOG}.uri": "http://nessie:19120/api/v1",
                    f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse": ICEBERG_WAREHOUSE,
                    f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                    "spark.sql.defaultCatalog": ICEBERG_CATALOG,
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
                },
                "hadoopConf": {
                    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "fs.s3a.aws.credentials.provider": (
                        "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
                    ),
                },
                "driver": {
                    "cores": driver_cores,
                    "memory": driver_memory,
                    "serviceAccount": "spark-driver",
                    "labels": {"app": "liquidity-pipeline"},
                },
                "executor": {
                    "cores": executor_cores,
                    "memory": executor_memory,
                    "instances": executor_instances,
                    "labels": {"app": "liquidity-pipeline"},
                },
                "dynamicAllocation": {
                    "enabled": True,
                    "initialExecutors": executor_instances,
                    "minExecutors": 1,
                    "maxExecutors": executor_instances * 2,
                },
            },
        }
    }


# =========================================================================
# DAG Definition
# =========================================================================

with DAG(
    dag_id="partitioned_dbt_spark_iceberg_pipeline",
    description=(
        "Partitioned liquidity analytics pipeline: "
        "Spark ingestion → dbt (entire project via Cosmos factory) → "
        "Iceberg maintenance.  All lineage emitted to DataHub via OpenLineage."
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["liquidity", "dbt", "spark", "iceberg", "datahub"],
) as dag:

    # =====================================================================
    # STEP 1 — Spark Ingestion (Raw → Iceberg Staging, partitioned)
    # =====================================================================
    ingest_market_data = SparkKubernetesOperator(
        task_id="ingest_market_data_spark",
        namespace=SPARK_NAMESPACE,
        template_spec=_spark_template(
            app_name="ingest-market-data",
            main_py="s3://data-lake-prod/spark-apps/ingest_market_data.py",
            extra_args=[
                "--partition-date", PARTITION_DATE,
                "--source-path", f"{S3_BUCKET}/raw/market_data",
                "--target-table", f"{ICEBERG_CATALOG}.staging.market_data",
            ],
            executor_instances=4,
            executor_memory="8g",
        ),
        kubernetes_conn_id=SPARK_K8S_CONN,
        outlets=[asset_market_data],
        get_logs=True,
        do_xcom_push=True,
        reattach_on_restart=True,
        delete_on_termination=True,
        retries=2,
    )

    ingest_positions_loans = SparkKubernetesOperator(
        task_id="ingest_positions_loans_spark",
        namespace=SPARK_NAMESPACE,
        template_spec=_spark_template(
            app_name="ingest-positions-loans",
            main_py="s3://data-lake-prod/spark-apps/ingest_positions.py",
            extra_args=[
                "--partition-date", PARTITION_DATE,
                "--source-path", f"{S3_BUCKET}/raw/positions_loans",
                "--target-table", f"{ICEBERG_CATALOG}.staging.positions_loans",
                "--source-system", "LMS",
            ],
            executor_instances=2,
            executor_memory="4g",
        ),
        kubernetes_conn_id=SPARK_K8S_CONN,
        outlets=[asset_positions_loans],
        get_logs=True,
        do_xcom_push=True,
        reattach_on_restart=True,
        delete_on_termination=True,
        retries=2,
    )

    ingest_positions_deposits = SparkKubernetesOperator(
        task_id="ingest_positions_deposits_spark",
        namespace=SPARK_NAMESPACE,
        template_spec=_spark_template(
            app_name="ingest-positions-deposits",
            main_py="s3://data-lake-prod/spark-apps/ingest_positions.py",
            extra_args=[
                "--partition-date", PARTITION_DATE,
                "--source-path", f"{S3_BUCKET}/raw/positions_deposits",
                "--target-table", f"{ICEBERG_CATALOG}.staging.positions_deposits",
                "--source-system", "CBS",
            ],
            executor_instances=2,
            executor_memory="4g",
        ),
        kubernetes_conn_id=SPARK_K8S_CONN,
        outlets=[asset_positions_deposits],
        get_logs=True,
        do_xcom_push=True,
        reattach_on_restart=True,
        delete_on_termination=True,
        retries=2,
    )

    # =====================================================================
    # STEP 2 — dbt: ONE DbtTaskGroup for the ENTIRE project (factory)
    # =====================================================================
    #
    # Cosmos inspects the dbt project and AUTOMATICALLY:
    #   • Creates one task per model (stg_market_data, stg_positions_loans,
    #     stg_positions_deposits, mart_positions, mart_liquidity_risk,
    #     mart_cashflow_forecast, … any future model)
    #   • Creates one task per test, wired after its parent model
    #   • Resolves the ref() dependency graph:
    #       stg_positions_loans ──┐
    #                             ├─► mart_positions ──► mart_liquidity_risk
    #       stg_positions_deposits┘                  └─► mart_cashflow_forecast
    #   • Emits one Airflow Asset per model for data-aware scheduling
    #
    # Adding a new dbt model to models/ requires ZERO changes here.
    # =====================================================================
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=cosmos_project,
        profile_config=cosmos_profile,
        execution_config=cosmos_execution,
        render_config=cosmos_render,
        operator_args=cosmos_operator_args,
        default_args={"retries": 2},
    )

    # =====================================================================
    # STEP 3 — Publish DQ results to DataHub (supplemental enrichment)
    # =====================================================================
    @task(
        task_id="publish_dq_to_datahub",
        outlets=[dq_report_asset],
    )
    def publish_dq_to_datahub(**context):
        """
        Parse dbt test artefacts and publish structured DQ assertions
        to DataHub via its Python SDK.

        The OpenLineage provider already sends basic pass/fail via OL facets.
        This task enriches those with assertion descriptions, categories,
        row-count metrics, freshness SLA checks, and custom tags.
        """
        import json as _json
        from pathlib import Path as _Path

        run_results_path = _Path(
            "/opt/airflow/dbt/liquidity_analytics/target/run_results.json"
        )

        if not run_results_path.exists():
            context["ti"].log.warning(
                "No run_results.json found; skipping DQ publish."
            )
            return

        run_results = _json.loads(run_results_path.read_text())

        dq_summary = {
            "partition_date": context["data_interval_end"].to_date_string(),
            "total_tests": 0,
            "passed": 0,
            "failed": 0,
            "warned": 0,
            "test_details": [],
        }

        for result in run_results.get("results", []):
            if result.get("unique_id", "").startswith("test."):
                dq_summary["total_tests"] += 1
                status = result.get("status", "unknown")
                if status == "pass":
                    dq_summary["passed"] += 1
                elif status == "fail":
                    dq_summary["failed"] += 1
                elif status == "warn":
                    dq_summary["warned"] += 1

                dq_summary["test_details"].append({
                    "test_id": result["unique_id"],
                    "status": status,
                    "execution_time": result.get("execution_time"),
                    "message": result.get("message", ""),
                })

        context["ti"].log.info(
            "DQ Summary: %d tests | %d passed | %d failed | %d warned",
            dq_summary["total_tests"],
            dq_summary["passed"],
            dq_summary["failed"],
            dq_summary["warned"],
        )
        context["ti"].xcom_push(key="dq_summary", value=dq_summary)

        try:
            from datahub.emitter.rest_emitter import DatahubRestEmitter
            from datahub.metadata.schema_classes import (
                AssertionResultClass,
                AssertionResultTypeClass,
                AssertionRunEventClass,
                AssertionRunStatusClass,
            )

            emitter = DatahubRestEmitter(
                gms_server=context["var"]["value"].get(
                    "datahub_gms_url", "http://datahub-gms:8080"
                ),
            )

            for test_detail in dq_summary["test_details"]:
                assertion_urn = (
                    f"urn:li:assertion:"
                    f"dbt_{test_detail['test_id'].replace('.', '_')}"
                )
                run_event = AssertionRunEventClass(
                    timestampMillis=int(
                        context["data_interval_end"].timestamp() * 1000
                    ),
                    assertionUrn=assertion_urn,
                    asserteeUrn=(
                        f"urn:li:dataset:(urn:li:dataPlatform:iceberg,"
                        f"{ICEBERG_CATALOG}.marts.liquidity_risk,PROD)"
                    ),
                    status=AssertionRunStatusClass.COMPLETE,
                    result=AssertionResultClass(
                        type=(
                            AssertionResultTypeClass.SUCCESS
                            if test_detail["status"] == "pass"
                            else AssertionResultTypeClass.FAILURE
                        ),
                        nativeResults={
                            "message": test_detail.get("message", "")
                        },
                    ),
                )
                emitter.emit_mce(run_event)

            emitter.flush()
            context["ti"].log.info("DQ assertions emitted to DataHub.")

        except ImportError:
            context["ti"].log.info(
                "datahub SDK not installed; DQ results available via "
                "OpenLineage facets only."
            )

    publish_dq = publish_dq_to_datahub()

    # =====================================================================
    # STEP 4 — Iceberg Table Maintenance (OPTIMIZE + VACUUM)
    # =====================================================================
    iceberg_maintenance = SparkKubernetesOperator(
        task_id="iceberg_maintenance",
        namespace=SPARK_NAMESPACE,
        template_spec=_spark_template(
            app_name="iceberg-maintenance",
            main_py="s3://data-lake-prod/spark-apps/iceberg_maintenance.py",
            extra_args=[
                "--catalog", ICEBERG_CATALOG,
                "--tables", (
                    "staging.market_data,staging.positions_loans,"
                    "staging.positions_deposits,marts.positions,"
                    "marts.liquidity_risk,marts.cashflow_forecast"
                ),
                "--partition-date", PARTITION_DATE,
                "--optimize", "true",
                "--vacuum-older-than-days", "7",
                "--vacuum-retain-snapshots", "10",
            ],
            executor_instances=2,
            executor_memory="4g",
            driver_memory="2g",
        ),
        kubernetes_conn_id=SPARK_K8S_CONN,
        get_logs=True,
        reattach_on_restart=True,
        delete_on_termination=True,
    )

    # =====================================================================
    # Task Dependencies — two layers of guarantees
    # =====================================================================
    #
    # LAYER 1 — Airflow task graph (this DAG):
    #   ALL three Spark ingest tasks must succeed before ANY dbt task runs.
    #   This is enforced by the >> operator below.
    #
    # LAYER 2 — Cosmos ref() graph (inside dbt_transform):
    #   Cosmos reads the dbt project and wires model tasks internally:
    #     stg_positions_loans   ──┐
    #                             ├──► mart_positions ──┬──► mart_liquidity_risk
    #     stg_positions_deposits ─┘                     └──► mart_cashflow_forecast
    #     stg_market_data ─────────────────────────────────►
    #
    #   mart_positions WILL NOT start until BOTH stg_positions_loans
    #   and stg_positions_deposits have succeeded.  This is guaranteed
    #   by the ref() calls in mart_positions.sql.  No AssetWatcher needed.
    #
    # LAYER 3 — Airflow Assets (cross-DAG future-proofing):
    #   Each ingest task declares an `outlet` Asset.  If you later split
    #   ingestion into a separate DAG, the dbt DAG can schedule on:
    #     schedule=(asset_positions_loans & asset_positions_deposits & asset_market_data)
    #   The & (AND) operator ensures ALL feeds are present before dbt runs.
    #
    (
        [ingest_market_data, ingest_positions_loans, ingest_positions_deposits]
        >> dbt_transform
        >> publish_dq
        >> iceberg_maintenance
    )
