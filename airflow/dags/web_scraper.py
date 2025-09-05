# dags/init_3_web_scraping_gcp_vm_dag.py
import os, json, requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago

URL = "https://cycling.data.tfl.gov.uk"
LOCAL_DICT_FILE = "/tmp/links_dictionary.json"

# Configure via .env (docker-compose passes env into containers)
GCS_BUCKET = os.environ.get("GCS_BUCKET", "REPLACE_ME")
GCS_DEST_PREFIX = os.environ.get("GCS_DEST_PREFIX", "raw/tfl/usage-stats")

# Years to keep (same logic you had)
FILTER_YEARS = [2021, 2022]

def fetch_html(**kwargs):
    r = requests.get(URL, timeout=30)
    r.raise_for_status() # raise error for bad responses (e.g., 404, 500) and stop the DAG run
    kwargs["ti"].xcom_push(key="html_content", value=r.text) # kwargs["ti"] object writes the text content of url in xcom for next task to use

def parse_links(**kwargs):
    html = kwargs["ti"].xcom_pull(key="html_content", task_ids="download_contents_task") # pull the downloaded html from previous task
    soup = BeautifulSoup(html, "html.parser") # parse the html content using BeautifulSoup

    table = soup.find("table") # find the first table in the html
    tbody = table.find("tbody") # find the tbody within the table

    folder_name = "usage-stats/" # the folder we want to scrape links from
    capture = False # flag to start capturing links once we find the folder_name
    extracted = {} # dictionary to hold the extracted links

    for row in tbody.find_all("tr"): # iterate over each row in the table body
        cols = row.find_all("td") # get all columns in the row

        if not capture: # if we haven't started capturing yet(if capture is not True) OR if capture is False, since capture is False initially, below block runs
            vals = [c.text.strip() for c in cols] # get the text of each column
            if vals and vals[0] == folder_name: # check if the first column matches our folder_name
                capture = True # set capture to True to start capturing links from next rows if folder_name matches
            continue

        name = cols[0].text.strip() # after capture is True, get the name of the file from the first column e.g usage-stats/17Mar2021-23Mar2021.csv
        name_wo_ext = name.split(".")[-2] # remove the file extension (e.g., .csv) i.e counting from the right, you have usage-stats/17Mar2021-23Mar2021
        year = name_wo_ext[-4:] # extract the year from the last 4 characters you get 2021

        if not (year.isdigit() and int(year) in FILTER_YEARS): # if year is not a digit or year is not in FILTER_YEARS, skip this row and continue the loop
            continue

        # e.g., ...17Mar2021-23Mar2021.csv -> "23Mar2021"
        last_date = name_wo_ext.split("-")[-1] # get the last date part after the hyphen
        extracted[last_date] = cols[0].a["href"]  # for each completed loop, create an entry in extracted dictionary key:value (last_date:href)                                                   # relative href: 'usage-stats/xxx.csv'

    kwargs["ti"].xcom_push(key="links_dict", value=extracted) # push the extracted dictionary to xcom for next task to use

def write_json(**kwargs):
    
    links_dict = kwargs["ti"].xcom_pull(key="links_dict", task_ids="extract_links_task") or {} # grab the extracted dictionary and name it again as links_dict
    with open(LOCAL_DICT_FILE, "w", encoding="utf-8") as f: # create a file with the path stated in LOCAL_DICT_FILE string variable
        json.dump(links_dict, f, indent=4) # write the links_dict to the file in json format with indentation of 4 spaces

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
