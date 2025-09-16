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

Weekly journey files (actual rides)
I scraped the TfL cycling data page to collect links under usage-stats/, then downloaded each weekly CSV.
Source: Transport for London (TfL) 
 (the homepage I scraped to build links_dictionary.json), which points to files like usage-stats/17Mar2021-23Mar2021.csv.

Docking stations (station lookup/dimension)
CSV of docking stations (IDs, names; coordinates may be refined later).
Source: https://www.whatdotheyknow.com/request/664717/response/1572474/attach/3/Cycle%20hire%20docking%20stations.csv.txt

Daily weather (weather dimension)
JSON of daily weather, normalized in the pipeline to an array of day records.
Source (direct download link): https://docs.google.com/uc?export=download&id=13LWAH93xxEvOukCnPhrfXH7rZZq_-mss

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
