# dags/init_3_web_scraping_gcp_vm_dag.py
import os, json, requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago

URL = "https://cycling.data.tfl.gov.uk"
LOCAL_DICT_FILE = "/tmp/links_dictionary.json"

GCS_BUCKET = os.environ.get("GCS_BUCKET", "REPLACE_ME")
GCS_DEST_PREFIX = os.environ.get("GCS_DEST_PREFIX", "raw/tfl/usage-stats")

FILTER_YEARS = [2021, 2022, 2023, 2024, 2025]

def fetch_html(**kwargs):
    # List objects under usage-stats/ from the public S3 bucket
    list_url = "https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk"
    params = {"list-type": "2", "prefix": "usage-stats/", "max-keys": "5000"}
    r = requests.get(list_url, params=params, timeout=30)
    r.raise_for_status()
    # push XML into the same XCom key your next task expects
    kwargs["ti"].xcom_push(key="html_content", value=r.text)

def parse_links(**kwargs):
    xml_text = kwargs["ti"].xcom_pull(key="html_content", task_ids="download_contents_task")
    soup = BeautifulSoup(xml_text, "xml")  # parse XML, not HTML

    extracted = {}
    for obj in soup.find_all("Contents"):
        key_tag = obj.find("Key")
        if not key_tag:
            continue
        key = key_tag.text  # e.g. 'usage-stats/17Mar2021-23Mar2021.csv'
        if not (key.startswith("usage-stats/") and (key.endswith(".csv") or key.endswith(".xlsx"))):
            continue

        # Keep your original year filtering logic
        name_wo_ext = key.split(".")[-2]   # 'usage-stats/17Mar2021-23Mar2021'
        year = name_wo_ext[-4:]            # '2021'
        if not (year.isdigit() and int(year) in FILTER_YEARS):
            continue

        last_date = name_wo_ext.split("-")[-1]  # '23Mar2021'
        extracted[last_date] = key              # store relative href as before

    if not extracted:
        raise ValueError(f"No links found under usage-stats/ for FILTER_YEARS={FILTER_YEARS}")

    kwargs["ti"].xcom_push(key="links_dict", value=extracted)

def write_json(**kwargs):
    links_dict = kwargs["ti"].xcom_pull(key="links_dict", task_ids="extract_links_task") or {}
    with open(LOCAL_DICT_FILE, "w", encoding="utf-8") as f:
        json.dump(links_dict, f, indent=4)

default_args = {"owner": "airflow", "start_date": days_ago(1), "retries": 1}

with DAG(
    dag_id="init_3_web_scraping_gcp_vm_dag",
    description="Scrape TfL usage-stats links and upload links_dictionary.json to GCS.",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "tfl", "web-scraping"],
) as dag:

    download_contents_task = PythonOperator(
        task_id="download_contents_task",
        python_callable=fetch_html,
        provide_context=True,
    )

    extract_links_task = PythonOperator(
        task_id="extract_links_task",
        python_callable=parse_links,
        provide_context=True,
    )

    export_links_task = PythonOperator(
        task_id="export_links_task",
        python_callable=write_json,
        provide_context=True,
    )

    upload_dict_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_dict_to_gcs",
        src=LOCAL_DICT_FILE,
        dst=f"{GCS_DEST_PREFIX}/links_dictionary.json",
        bucket=GCS_BUCKET,
        mime_type="application/json",
    )

    download_contents_task >> extract_links_task >> export_links_task >> upload_dict_to_gcs