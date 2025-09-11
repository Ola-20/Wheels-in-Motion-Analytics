# dags/init_1_spark_dataproc_dag.py
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# ---- Config via env
PROJECT_ID     = os.environ.get("GCP_PROJECT_ID", "bicycle-renting-proc-analytics")
REGION         = os.environ.get("GCP_REGION", "australia-southeast1")
CLUSTER_NAME   = os.environ.get("DATAPROC_CLUSTER_NAME", "extras-data-transformer")
GCS_SCRIPT_URI = os.environ.get(
    "GCS_PYSPARK_URI",
    "gs://your-bucket/utils/scripts/init-data-transformation.py",
)
STAGING_BUCKET = os.environ.get("DATAPROC_STAGING_BUCKET", "my-dataproc-staging")

# ---- Dataproc cluster config (single-node, staging bucket only)
CLUSTER_CONFIG = {
    "project_id": PROJECT_ID,
    "config": {
        "config_bucket": STAGING_BUCKET,   # staging bucket for temp + logs
        "gce_cluster_config": {
            "internal_ip_only": False,     # default public egress, fine for small project
            "service_account": "de-service-account@bicycle-renting-proc-analytics.iam.gserviceaccount.com",
            "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"]
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-4",
            "disk_config": {"boot_disk_size_gb": 100},
        },
        "worker_config": {"num_instances": 0},  # single-node cluster
        "software_config": {
            "image_version": "2.2-debian12",
        },
    },
}

# ---- Spark job spec
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {
        "cluster_name": "{{ ti.xcom_pull(task_ids='create_cluster') }}"
    },
    "pyspark_job": {
        "main_python_file_uri": GCS_SCRIPT_URI,
    },
}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="init_1_spark_dataproc_dag",
    description="One-off Spark job on Dataproc to process extra files in GCS.",
    schedule="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["spark", "dataproc", "extras", "london", "2021", "journey"],
) as dag:

    wait_for_ingestion = ExternalTaskSensor(
        task_id="sensor_for_init_0_ingestion_dag",
        external_dag_id="init_0_ingestion_to_gcs_dag",  # <-- update to match your ingestion DAG
        external_task_id="end",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=30,
        retries=2,
    )

    start = EmptyOperator(task_id="start")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=PROJECT_ID,
        location=REGION,
        job=PYSPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",  # always clean up
    )

    end = EmptyOperator(task_id="end")

    wait_for_ingestion >> start >> create_cluster >> submit_job >> delete_cluster >> end
