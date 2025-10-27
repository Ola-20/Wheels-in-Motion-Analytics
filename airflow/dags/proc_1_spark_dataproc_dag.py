import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# --- Env / Config ---
PROJECT_ID       = os.environ.get("GCP_PROJECT_ID", "bicycle-renting-proc-analytics")
REGION           = os.environ.get("GCP_REGION", "australia-southeast1")
GCS_BUCKET       = os.environ.get("GCS_BUCKET", "bicycle-renting-proc-analytics-bucket-12345")
TMP_BUCKET       = os.environ.get("DATAPROC_TMP_BUCKET", "my-dataproc-staging")  # keep in same region
RUNTIME_VERSION  = os.environ.get("DATAPROC_RUNTIME", "2.2")

# PySpark driver script in GCS (uploaded earlier)
GCS_PYSPARK_URI_JOURNEY = os.environ.get(
    "GCS_PYSPARK_URI_JOURNEY",
    f"gs://{GCS_BUCKET}/utils/scripts/journey-data-transformation.py",
)

# Inputs/Outputs used by your script
INPUT_PREFIX   = os.environ.get("JOURNEY_INPUT_PREFIX",  "raw/cycling-journey/*/*.csv")
PROCESSED_DIM  = os.environ.get("PROCESSED_DIM",         "processed/cycling-dimension")
PROCESSED_FACT = os.environ.get("PROCESSED_FACT",        "processed/cycling-fact")

default_args = {"owner": "airflow", "start_date": days_ago(1), "retries": 0}

with DAG(
    dag_id="proc_1_spark_dataproc_serverless_dag",
    description="Run journey transformation on Dataproc Serverless (raw CSVs in GCS → Parquet dims/facts).",
    schedule=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["spark", "dataproc", "serverless", "journeys"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ---- CPU-CAPPED CONFIG (<= 7 vCPUs) ----
    # Formula: driver.cores + executor.cores * maxExecutors (dynamic)
    #          driver.cores + executor.cores * executor.instances (static)
    batch_body = {
        "pyspark_batch": {
            "main_python_file_uri": GCS_PYSPARK_URI_JOURNEY,
            "args": [
                "--gcs-bucket",     GCS_BUCKET,
                "--input-prefix",   INPUT_PREFIX,
                "--processed-dim",  PROCESSED_DIM,
                "--processed-fact", PROCESSED_FACT,
            ],
        },
        "runtime_config": {
            "version": RUNTIME_VERSION,
            "properties": {
                # Minimum allowed on Serverless:
                "spark.driver.cores": "4",
                "spark.executor.cores": "4",
                # Turn OFF autoscaling and pin to the minimum:
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": "2",
                # Reasonable small-ish memory/shuffle:
                "spark.driver.memory": "4g",
                "spark.executor.memory": "4g",
                "spark.sql.shuffle.partitions": "64",
            },
        },
        "environment_config": {"execution_config": {"staging_bucket": TMP_BUCKET}},
        "labels": {"component": "journey-transform", "env": "serverless"},
    }

    journey_transform = DataprocCreateBatchOperator(
        task_id="journey_transform_batch",
        project_id=PROJECT_ID,                 # ensure this is the PROJECT, not the bucket
        region=REGION,
        batch=batch_body,
        batch_id="journey-{{ ds_nodash }}-{{ ts_nodash[-6:] }}",
        gcp_conn_id="google_cloud_default",
        # deferrable=True,  # if your provider supports it
    )

    end = EmptyOperator(task_id="end")

    start >> journey_transform >> end