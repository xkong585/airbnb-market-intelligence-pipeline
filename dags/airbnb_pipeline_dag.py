"""
Airflow DAG: Airbnb Silver → Gold → Snowflake pipeline

Topology:
    silver  >>  gold  >>  snowflake_ingest

Run manually:  airflow dags trigger airbnb_pipeline
Schedule:      set SCHEDULE below (default: None = manual only)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ─────────────────────────────────────────
# Paths  (adjust to your VM layout)
# ─────────────────────────────────────────
SCRIPTS_DIR = "/shared/final_project/scripts"

SPARK_SUBMIT = "spark-submit"

SEDONA_PKG = "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.8.1"

# Schedule: None = manual trigger only.
# Examples: "@daily", "@weekly", "0 3 * * *"
SCHEDULE = None

# ─────────────────────────────────────────
# DAG definition
# ─────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="airbnb_pipeline",
    description="Airbnb Bronze → Silver → Gold → Snowflake (Spark + Sedona)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=SCHEDULE,
    catchup=False,
    default_args=default_args,
    tags=["airbnb", "spark", "etl"],
) as dag:

    # ── Task 1: Silver (Spark + Sedona geospatial join) ───────────────────
    silver = BashOperator(
        task_id="silver",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--packages {SEDONA_PKG} "
            f"--conf spark.sql.shuffle.partitions=200 "
            f"--conf spark.sql.autoBroadcastJoinThreshold=-1 "
            f"{SCRIPTS_DIR}/silver_pipeline.py"
        ),
        execution_timeout=timedelta(hours=2),
    )

    # ── Task 2: Gold (plain PySpark, no extra packages) ───────────────────
    gold = BashOperator(
        task_id="gold",
        bash_command=(
            f"{SPARK_SUBMIT} "
            f"--conf spark.sql.shuffle.partitions=200 "
            f"{SCRIPTS_DIR}/gold_pipeline.py"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # ── Task 3: Snowflake ingest (load gold tables into Snowflake) ────────
    # Credentials are read from environment variables (set via config.env)
    snowflake_ingest = BashOperator(
        task_id="snowflake_ingest",
        bash_command=f"python3 {SCRIPTS_DIR}/snowflake_ingest.py",
        execution_timeout=timedelta(hours=1),
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    silver >> gold >> snowflake_ingest
