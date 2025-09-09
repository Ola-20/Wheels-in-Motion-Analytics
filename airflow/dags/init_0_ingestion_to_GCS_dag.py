import os
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

# =========================
# ENV / CONFIG
# =========================
# Set these in your .env or container env
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

# GCS config
GCS_BUCKET = os.environ.get("GCS_BUCKET", "gcs_no_bucket")
# e.g. raw/cycling-extras
GCS_DEST_PREFIX = os.environ.get("GCS_DEST_PREFIX", "raw/cycling-extras")
# e.g. utils/scripts
GCS_SCRIPT_DESTINATION = os.environ.get("GCS_SCRIPT_DESTINATION", "utils/scripts")
GCP_CONN_ID = os.environ.get("GCP_CONN_ID", "google_cloud_default")

download_links = [
    {
        "name": "stations",
        "link": 'https://www.whatdotheyknow.com/request/664717/response/1572474/attach/3/Cycle%20hire%20docking%20stations.csv.txt',
        "output": "stations.csv",
    },
    {
        "name": "weather",
        # note: Google Drive direct download with cert flag to silence wget warnings
        "link": '--no-check-certificate "https://docs.google.com/uc?export=download&id=13LWAH93xxEvOukCnPhrfXH7rZZq_-mss"',
        "output": "weather.json",
    },
    {
        "name": "journey",
        "link": "https://cycling.data.tfl.gov.uk/usage-stats/246JourneyDataExtract30Dec2020-05Jan2021.csv",
        "output": "journey.csv",
    },
]

# i used this to upload helper scripts to GCS for use by other resources like dataproc etc
local_scripts = ["init-data-transformation.py", "journey-data-transformation.py"]



# =========================
def preprocess_data(filepath: str) -> None:
    """
    For weather.json, extract only the 'days' array and overwrite the file.
    No-op for other files.
    """
    filename = os.path.basename(filepath)
    if filename != "weather.json":
        print(f"No preprocessing needed for {filename}")
        return

    with open(filepath, "r") as f:
        weather = json.load(f)

    daily_weather = weather.get("days", [])
    with open(filepath, "w") as f:
        json.dump(daily_weather, f)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# =========================
# DAG
# =========================
with DAG(
    dag_id="init_0_ingestion_to_gcs_dag",
    description=(
        "Ingest extra cycling files (docking stations, weather, and a sample journey file) "
        "and upload them to Google Cloud Storage."
    ),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=["gcs", "weather", "stations", "london", "2021", "journey"],
) as dag:

    start = EmptyOperator(task_id="start")

    # -------------------------
    # Download + Preprocess
    # -------------------------
    with TaskGroup("download_files", tooltip="Download → optional preprocess") as download_section:
        for item in download_links:
            local_path = f"{path_to_local_home}/{item['output']}"

            download_task = BashOperator(
                task_id=f"download_{item['name']}_task",
                bash_command=f"wget {item['link']} -O {local_path}",
            )

            # Only preprocess weather.json
            if item["output"] == "weather.json":
                preprocessing_task = PythonOperator(
                    task_id="extract_daily_weather_data",
                    python_callable=preprocess_data,
                    op_kwargs={"filepath": local_path},
                )
                download_task >> preprocessing_task

    # -------------------------
    # Upload to GCS
    # -------------------------
    with TaskGroup("upload_files_to_gcs") as upload_section:
        for item in download_links:
            LocalFilesystemToGCSOperator(
                task_id=f"upload_{item['name']}_to_gcs_task",
                filename=f"{path_to_local_home}/{item['output']}",
                destination_bucket=GCS_BUCKET,
                destination_object=f"{GCS_DEST_PREFIX}/{item['output']}",
                gcp_conn_id=GCP_CONN_ID,
            )

    # -------------------------
    # Cleanup local files
    # -------------------------
    cleanup = BashOperator(
        task_id="cleanup_local_storage",
        bash_command=f"rm -f {path_to_local_home}/*.json {path_to_local_home}/*.csv || true",
    )

    # -------------------------
    # Upload local scripts to GCS
    # -------------------------
    with TaskGroup("upload_scripts_to_gcs") as upload_scripts_section:
        for idx, script_name in enumerate(local_scripts):
            LocalFilesystemToGCSOperator(
                task_id=f"upload_script_{idx}_to_gcs_task",
                # create string path using os.path.join to ensure cross-platform compatibility
                filename=os.path.join("dags", "scripts", script_name),
                destination_bucket=GCS_BUCKET,
                destination_object=f"{GCS_SCRIPT_DESTINATION}/{script_name}",
                gcp_conn_id=GCP_CONN_ID,
            )

    end = EmptyOperator(task_id="end")

    # Flow
    start >> download_section >> upload_section >> cleanup >> end
    start >> upload_scripts_section >> end