# dags/proc_0_ingestion_to_gcs_dag.py
import os
import json
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# --- Config / Constants ---
BASE_URL = "https://cycling.data.tfl.gov.uk/"
path_to_local_home   = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

GCS_BUCKET           = os.environ.get("GCS_BUCKET", "gcs_no_bucket")
# Monthly folder; templated by Airflow using the task's logical date, e.g. "raw/cycling-journey/Mar2025"
GCS_DESTINATION      = "raw/cycling-journey/{{ logical_date.strftime('%b%Y') }}"

# Where links_dictionary.json (from init_3) was uploaded, e.g. "raw/tfl/usage-stats"
GCS_MANIFEST_PREFIX  = os.environ.get("GCS_DEST_PREFIX", "raw/tfl/usage-stats")
GCP_CONN_ID          = os.environ.get("GCP_CONN_ID", "google_cloud_default")

# gs://<bucket>/<prefix>/links_dictionary.json
MANIFEST_OBJECT = f"{GCS_MANIFEST_PREFIX}/links_dictionary.json"


from datetime import datetime

def get_file_link(exec_date: str, gcs_destination_folder: str):
    ctx = get_current_context(); ti = ctx["ti"]
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    data = hook.download(bucket_name=GCS_BUCKET, object_name=MANIFEST_OBJECT)
    links = json.loads(data.decode("utf-8"))

    if exec_date not in links:
        latest = max(links.keys(), key=lambda s: datetime.strptime(s, "%d%b%Y"))
        print(f"[get_file_link] '{exec_date}' not in manifest; using latest '{latest}'")
        exec_date = latest

    rel_path  = links[exec_date]
    file_link = f"{BASE_URL.rstrip('/')}/{rel_path.lstrip('/')}"
    filename  = rel_path.split("/")[-1]
    local_path = f"{path_to_local_home}/{filename}"
    gcs_object = f"{gcs_destination_folder}/{filename}"

    ti.xcom_push(key="remote_file_link", value=file_link)
    ti.xcom_push(key="filename", value=filename)
    ti.xcom_push(key="local_file_link", value=local_path)
    ti.xcom_push(key="gcs_filepath_destination", value=gcs_object)

# Download command (quiet, follow redirects, fail on HTTP errors)
download_cmd = 'curl -sSLf "$link" > "$destination"'

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="proc_0_ingestion_to_gcs",
    description="Read manifest → download weekly CSV → upload to GCS (partitioned monthly)",
    default_args=default_args,
    schedule=None,          # trigger when you want (or set '@daily' if preferred)
    catchup=False,
    render_template_as_native_obj=True,
) as dag:

    # Let caller override the exec date via dag_run.conf.exec_date; otherwise use logical_date (DDMonYYYY)
    exec_date_expr = "{{ dag_run.conf.get('exec_date') or logical_date.strftime('%d%b%Y') }}"

    get_file_link_task = PythonOperator(
        task_id="get_file_link",
        python_callable=get_file_link,
        op_kwargs={
            "exec_date": exec_date_expr,
            "gcs_destination_folder": GCS_DESTINATION,
        },
    )

    download_file = BashOperator(
        task_id="download_file",
        bash_command=download_cmd,
        env={
            # Pull from XComs produced by get_file_link
            "link": "{{ ti.xcom_pull(task_ids='get_file_link', key='remote_file_link') }}",
            "destination": "{{ ti.xcom_pull(task_ids='get_file_link', key='local_file_link') }}",
        },
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="{{ ti.xcom_pull(task_ids='get_file_link', key='local_file_link') }}",
        dst="{{ ti.xcom_pull(task_ids='get_file_link', key='gcs_filepath_destination') }}",
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONN_ID,
        mime_type="text/csv",
    )

    cleanup_local = BashOperator(
        task_id="cleanup_local",
        bash_command='rm -f "{{ ti.xcom_pull(task_ids=\'get_file_link\', key=\'local_file_link\') }}" || true',
        trigger_rule="all_done",
    )

    get_file_link_task >> download_file >> upload_to_gcs >> cleanup_local