import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

PROJECT_ID        = os.environ.get("GCP_PROJECT_ID", "bicycle-renting-proc-analytics")
REGION            = os.environ.get("GCP_REGION", "australia-southeast1")
GCS_BUCKET        = os.environ.get("GCS_BUCKET", "bicycle-renting-proc-analytics-bucket-12345")
RAW_PREFIX        = os.environ.get("GCS_DEST_PREFIX", "raw/tfl/usage-stats")
PROCESSED_PREFIX  = os.environ.get("PROCESSED_PREFIX", "processed/cycling-dimension")
GCS_PYSPARK_URI   = os.environ.get("GCS_PYSPARK_URI")
DATAPROC_TMP_BUCKET = os.environ.get("DATAPROC_TMP_BUCKET", "my-dataproc-staging")
RUNTIME_VERSION   = os.environ.get("DATAPROC_RUNTIME", "2.2")

default_args = {"owner": "airflow", "start_date": days_ago(1), "retries": 0}

with DAG(
    dag_id="init_1_spark_dataproc_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["spark","dataproc","serverless"]
) as dag:

    wait_for_ingestion = ExternalTaskSensor(
        task_id="sensor_for_init_0_ingestion_dag",
        external_dag_id="init_0_ingestion_to_gcs_dag",
        external_task_id="end",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=30,
        retries=2,
    )

    start = EmptyOperator(task_id="start")

    batch_body = {
        "pyspark_batch": {
            "main_python_file_uri": GCS_PYSPARK_URI,
            "args": [
                "--gcs-bucket", GCS_BUCKET,
                "--raw-prefix", RAW_PREFIX,
                "--processed-prefix", PROCESSED_PREFIX,
            ],
        },
        "runtime_config": {"version": RUNTIME_VERSION},
        "environment_config": {"execution_config": {"staging_bucket": DATAPROC_TMP_BUCKET}},
        "labels": {"airflow_dag": "init_1_spark_dataproc_dag", "component": "serverless-spark"},
    }

    spark_serverless = DataprocCreateBatchOperator(
        task_id="spark_serverless_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch=batch_body,
        # optional: stable id while testing
        # batch_id="extras-transform-{{ ds_nodash }}",
    )

    end = EmptyOperator(task_id="end")

    wait_for_ingestion >> start >> spark_serverless >> end