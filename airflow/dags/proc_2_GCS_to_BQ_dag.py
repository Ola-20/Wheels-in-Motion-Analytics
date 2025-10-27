# dags/proc_2_gcs_to_bigquery_dag.py
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import DagRun
from airflow.utils.session import create_session  # ← added

# --- Env / config ---
PROJECT_ID  = os.environ.get("GCP_PROJECT_ID", "bicycle-renting-proc-analytics")
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "gcs_no_bucket")
BQ_DATASET  = os.environ.get("BQ_DATASET", "cycling_analytics")
BQ_LOCATION = os.environ.get("BQ_LOCATION", None)  # e.g. "australia-southeast1"
GCP_CONN_ID = os.environ.get("GCP_CONN_ID", "google_cloud_default")

# Folder layout in GCS (produced by your serverless transforms)
GCS_KEY_DIMS = "processed/cycling-dimension"
GCS_KEY_FACT = "processed/cycling-fact"

gcs_objects = [
    {"type": "stations", "key": GCS_KEY_DIMS, "subpath": "stations/", "table": "dim_station"},
    {"type": "datetime", "key": GCS_KEY_DIMS, "subpath": "datetime/", "table": "dim_datetime"},
    {"type": "journey",  "key": GCS_KEY_FACT, "subpath": "journey/",  "table": "fact_journey"},
]

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# 🔑 Always target the newest SUCCESSFUL run of proc_1 (version-safe signature)
def latest_success_exec_date_fn(logical_date=None, **kwargs):
    """
    Returns execution_date of the most recent SUCCESSFUL DagRun of proc_1.
    If none exist yet, returns None so the sensor keeps poking (mode='reschedule').
    Works with Airflow calling conventions that pass logical_date and kwargs (e.g. context).
    """
    with create_session() as session:
        dr = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "proc_1_spark_dataproc_serverless_dag",
                DagRun.state == "success",
            )
            .order_by(DagRun.execution_date.desc())
            .first()
        )
        return dr.execution_date if dr else None

with DAG(
    dag_id="proc_2_gcs_to_bigquery_dag",
    description="Load processed Parquet (dims & facts) from GCS into BigQuery (overwrite).",
    schedule=None,                 # still manual or upstream-triggered
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["gcs-to-bq", "dim", "fact", "cycling"],
) as dag:

    # Wait for the latest success of proc_1:end (no timestamp matching needed)
    wait_for_proc_1 = ExternalTaskSensor(
        task_id="wait_for_proc_1",
        external_dag_id="proc_1_spark_dataproc_serverless_dag",
        external_task_id="end",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_date_fn=latest_success_exec_date_fn,  # ← key change
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,   # 1 hour
        retries=2,
    )

    start = EmptyOperator(task_id="start")

    with TaskGroup("load_gcs_parquet_to_bq") as transfer_section:
        for item in gcs_objects:
            prefix = f"{item['key'].rstrip('/')}/{item['subpath'].lstrip('/')}"
            GCSToBigQueryOperator(
                task_id=f"load_{item['type']}_to_bq",
                bucket=GCS_BUCKET,
                source_objects=[f"{prefix}*.parquet"],
                destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{item['table']}",
                source_format="PARQUET",
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
                gcp_conn_id=GCP_CONN_ID,
                location=BQ_LOCATION,
            )

    end = EmptyOperator(task_id="end")

    wait_for_proc_1 >> start >> transfer_section >> end
