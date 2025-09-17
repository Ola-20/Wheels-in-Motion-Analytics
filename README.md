# Wheels in Motion Analytics
*Note: This project is still under development*

## Overview

This project turns Transport for London (TfL) bike-rental files into clean, analysis-ready tables in BigQuery. It uses Terraform to manage cloud resources provisioning, Airflow to schedule work, Dataproc Serverless (Spark) to transform data, Cloud Storage (GCS) to stage files, BigQuery for the final analytics layer, and Looker Studio for visualization.


## Objective

To analyze and understand how weather conditions, station characteristics, and time factors influence bicycle rental demand in London. The goal is to identify key patterns and relationships that can help predict rental usage and optimize resource allocation.

To achieve this, I built a reliable, repeatable pipeline from web → raw files → cleaned datasets → BigQuery.

   Produce a simple star-schema for analysis and dashboards:
   
   fact_journey (rides)
   
   dim_station (stations)
   
   dim_datetime (time attributes)
   
   dim_weather (daily weather)

## Datasets Used
1) Weekly journey files (actual rides)
   I scraped the TfL cycling data page to collect links under usage-stats/, then downloaded each weekly CSV.
   Source: [Transport for London (TfL)](https://cycling.data.tfl.gov.uk/) 
    (the homepage I scraped to build links_dictionary.json), which points to files like usage-stats/17Mar2021-23Mar2021.csv.

2) Docking stations (station lookup/dimension)
   CSV of docking stations (IDs, names; coordinates may be refined later).
   Source (direct download link): [here](https://www.whatdotheyknow.com/request/664717/response/1572474/attach/3/Cycle%20hire%20docking%20stations.csv.txt)

3) Daily weather (weather dimension)
   JSON of daily weather, normalized in the pipeline to an array of day records.
   Source (direct download link): [here](https://docs.google.com/uc?export=download&id=13LWAH93xxEvOukCnPhrfXH7rZZq_-mss)

### Process (two significant steps)
The project was segregated into two broad sections, containing two sets of scripts.

#### 1) Bootstrap (setup and test, Upload helper scripts)

It sets up the environment so your Airflow containers and connections can talk to GCP using the right credentials and settings.
Then it downloads the TfL files, cleans the weather JSON, uploads data and scripts to GCS, and runs a Dataproc Serverless job to write processed Parquet files.

The scripts are below:

1) init_0_ingestion_to_gcs_dag – Download seed files (stations, weather, one sample journey), normalize weather.json, and upload data + Spark scripts to GCS.
      
2) init_1_spark_dataproc_dag – Run a small Dataproc Serverless job to write stations and weather to Parquet in processed/….
      
3) init_2_gcs_to_bigquery_dag – Load processed weather into BigQuery as dim_weather.
      
4) init_3_web_scraping_gcp_vm_dag – Crawl the TfL site and save a manifest (links_dictionary.json) of weekly journey CSV links to GCS.

*Note: only this first section has been completed as of 18/09/2025. See the Airlow WebObserver UI below*

![Section One Completed](images/ingestion_initialisation_dag_works.png)

#### 2) Production (real data flow)

Section 2 (Transform)
It waits until Step 1 finishes, then creates a Dataproc Serverless batch with PySpark script and settings.
It reads raw files from GCS, transforms them, and writes clean Parquet data to the processed folder, using the staging bucket for temp file.

The scripts are below:

1) proc_0_ingestion_to_gcs_dag – Use the manifest to download the real weekly journey CSVs to GCS raw/.
   
2) proc_1_spark_dataproc_serverless_dag – Run the journey transform script to:
   
         create processed/cycling-fact/journey/ (fact),
         
         create processed/cycling-dimension/datetime/ (datetime dim),
         
         and augment processed/cycling-dimension/stations/ if new station IDs appear.

3) proc_2_gcs_to_bigquery_dag – Load Parquet from GCS to BigQuery tables: dim_station, dim_datetime, fact_journey (overwrite for a clean, idempotent load).

### Expected Outputs

   BigQuery tables: dim_weather, dim_station, dim_datetime, fact_journey.

### GCS directory layout:

   raw/… (downloaded weekly CSVs)
   
   processed/cycling-dimension/{stations, datetime, weather}/
   
   processed/cycling-fact/journey/
   
   utils/scripts/*.py, links_dictionary.json (manifest)
