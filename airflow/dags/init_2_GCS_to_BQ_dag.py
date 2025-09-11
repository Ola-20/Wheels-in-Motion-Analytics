# dags/init_2_gcs_to_bigquery_dag.py
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# ---- Config via env ----
GCS_BUCKET  = os.environ.get("GCS_BUCKET", "gcs_no_bucket")
BQ_DATASET  = os.environ.get("BQ_DATASET", "cycling_analytics")
GCP_CONN_ID = os.environ.get("GCP_CONN_ID", "google_cloud_default")

# Folder layout in GCS
GCS_KEY_EXTRAS = "processed/cycling-dimension"

# Files to load (only weather dim in this DAG)
gcs_objects = [
    {
        "type": "weather",
        "key": GCS_KEY_EXTRAS,   # folder/prefix
        "subpath": "weather/",   # subfolder inside key
        "table": "dim_weather",  # BigQuery table name
        "file_type": "parquet",
    }
]

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="init_2_gcs_to_bigquery_dag",
    description="Load dimension files from GCS to BigQuery (overwrite).",
    schedule="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=["weather", "gcs_to_bq"],
) as dag:

    # Wait for your Dataproc DAG to finish
    wait_for_init_1 = ExternalTaskSensor(
        task_id="sensor_for_init_1_spark_dag",
        poke_interval=30,
        soft_fail=False,
        retries=2,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        external_task_id="end",
        external_dag_id="init_1_spark_dataproc_dag",  # update if your DAG id differs
    )

    start = EmptyOperator(task_id="start")

    # Load from GCS -> BigQuery with overwrite (idempotent)
    with TaskGroup("load_files_to_bigquery") as transfer_section:
        for item in gcs_objects:
            GCSToBigQueryOperator(
                task_id=f"load_{item['type']}_gcs_to_bq", # name the task using the type
                bucket=GCS_BUCKET,
                source_objects=[f"{item['key']}/{item['subpath']}*.parquet"], # source of file in GCS using variable form gcs_dict above
                destination_project_dataset_table=f"{BQ_DATASET}.{item['table']}", # destinationtable  in BQ.
                source_format="PARQUET",
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",  # overwrite target table
                gcp_conn_id=GCP_CONN_ID,
            )

    end = EmptyOperator(task_id="end")

    wait_for_init_1 >> start >> transfer_section >> end