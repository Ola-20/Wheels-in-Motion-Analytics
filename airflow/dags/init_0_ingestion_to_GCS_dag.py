import os
import json
import logging
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
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

# GCS config
GCS_BUCKET = os.environ.get("GCS_BUCKET", "gcs_no_bucket")
GCS_DEST_PREFIX = os.environ.get("GCS_DEST_PREFIX", "raw/cycling-extras")
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
        # I used Google Drive direct download with cert flag to silence wget warnings
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
# Helpers
# =========================
def preprocess_data(filepath: str) -> None:
    """
    For weather.json, normalize payload to a list of day dicts and overwrite the file.
    Handles:
      - dict roots like {"days": [...]} or {"data": [...]}
      - list roots like [...]
      - JSON strings (loads to Python first)
    No-op for other files.
    """
    filename = os.path.basename(filepath)
    if filename != "weather.json":
        print(f"No preprocessing needed for {filename}")
        return

    # Read raw file (robust to stray whitespace/BOM)
    with open(filepath, "r", encoding="utf-8") as f:
        raw = f.read().strip()

    # Parse JSON safely
    try:
        payload = json.loads(raw)
    except Exception as e:
        logging.exception("Failed to parse %s as JSON; writing empty list", filename)
        payload = None

    # Normalize to list of days
    if isinstance(payload, dict):
        daily_weather = payload.get("days") or payload.get("data") or []
    elif isinstance(payload, list):
        daily_weather = payload
    else:
        daily_weather = []

    if not isinstance(daily_weather, list):
        logging.error("Unexpected weather format (%s); coercing to empty list", type(daily_weather))
        daily_weather = []

    if daily_weather:
        try:
            logging.info("weather.json sample keys: %s", list(daily_weather[0].keys()))
        except Exception:
            pass  # first element might not be a dict; ignore

    # Overwrite file with normalized list
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(daily_weather, f)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# Ensure local dir exists (sometimes AIRFLOW_HOME is not created)
os.makedirs(path_to_local_home, exist_ok=True)

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
                src=f"{path_to_local_home}/{item['output']}",
                bucket=GCS_BUCKET,
                dst=f"{GCS_DEST_PREFIX}/{item['output']}",
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
                src=os.path.join("dags", "scripts", script_name),
                bucket=GCS_BUCKET,
                dst=f"{GCS_SCRIPT_DESTINATION}/{script_name}",
                gcp_conn_id=GCP_CONN_ID,
            )

    end = EmptyOperator(task_id="end")

    # Flow
    start >> download_section >> upload_section >> cleanup >> end
    start >> upload_scripts_section >> end
