### Overview

This project turns Transport for London (TfL) bike-rental files into clean, analysis-ready tables in BigQuery. It uses Airflow to schedule work, Dataproc Serverless (Spark) to transform data, Cloud Storage (GCS) to stage files, and BigQuery for the final analytics layer.

### Goal

Build a reliable, repeatable pipeline from web → raw files → cleaned datasets → BigQuery.

Produce a simple star-schema for analysis and dashboards:

fact_journey (rides)

dim_station (stations)

dim_datetime (time attributes)

dim_weather (daily weather)

### Datasets Used

TfL weekly journey CSVs (from the usage-stats/ folder): ride-level data such as Rental Id, Bike Id, Start/End Date, Start/End Station Id/Name, etc.

Docking stations CSV: station IDs, names (and placeholders for coordinates if missing).

Daily weather JSON: normalized to a list of daily records and used to build dim_weather.

### Process (two big steps)
#### 1) Bootstrap (setup + helpers)

init_0_ingestion_to_gcs_dag – Download seed files (stations, weather, one sample journey), normalize weather.json, and upload data + Spark scripts to GCS.

init_1_spark_dataproc_dag – Run a small Dataproc Serverless job to write stations and weather to Parquet in processed/….

init_2_gcs_to_bigquery_dag – Load processed weather into BigQuery as dim_weather.

init_3_web_scraping_gcp_vm_dag – Crawl the TfL site and save a manifest (links_dictionary.json) of weekly journey CSV links to GCS.

#### 2) Production (real data flow)

proc_0_ingestion_to_gcs_dag – Use the manifest to download the real weekly journey CSVs to GCS raw/.

proc_1_spark_dataproc_serverless_dag – Run the journey transform script to:

create processed/cycling-fact/journey/ (fact),

create processed/cycling-dimension/datetime/ (datetime dim),

and augment processed/cycling-dimension/stations/ if new station IDs appear.

proc_2_gcs_to_bigquery_dag – Load Parquet from GCS to BigQuery tables: dim_station, dim_datetime, fact_journey (overwrite for a clean, idempotent load).

Outputs

BigQuery tables: dim_weather, dim_station, dim_datetime, fact_journey.

GCS layout:

raw/… (downloaded weekly CSVs)

processed/cycling-dimension/{stations, datetime, weather}/

processed/cycling-fact/journey/

utils/scripts/*.py, links_dictionary.json (manifest)
