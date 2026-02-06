# ============================================================
# Dockerfile — Airflow 3.1 + dbt + Cosmos + DataHub
# ============================================================
# Build: docker build -t liquidity-airflow:latest .
# Run:   docker compose up -d
# ============================================================

FROM apache/airflow:3.1.7-python3.11

USER root

# System dependencies for dbt-spark (PyHive / Thrift)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc g++ libsasl2-dev libsasl2-modules && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# dbt project
COPY dbt/ /opt/airflow/dbt/

# Install dbt packages
RUN cd /opt/airflow/dbt/liquidity_analytics && \
    dbt deps --profiles-dir /opt/airflow/dbt

# Airflow DAGs
COPY dags/ /opt/airflow/dags/

# OpenLineage config
COPY config/airflow_openlineage.cfg /opt/airflow/airflow_openlineage.cfg
