#!/usr/bin/env python3
# One-time data transformation for stations & weather (GCS version)

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

# ----------------------------
# Config (set via env if you like)
# ----------------------------
GCS_BUCKET         = os.environ.get("GCS_BUCKET", "your-bucket-name")
RAW_PREFIX         = os.environ.get("RAW_PREFIX", "raw/cycling-extras")
PROCESSED_PREFIX   = os.environ.get("PROCESSED_PREFIX", "processed/cycling-dimension")

STATIONS_IN  = f"gs://{GCS_BUCKET}/{RAW_PREFIX}/stations.csv"
WEATHER_IN   = f"gs://{GCS_BUCKET}/{RAW_PREFIX}/weather.json"
STATIONS_OUT = f"gs://{GCS_BUCKET}/{PROCESSED_PREFIX}/stations/"
WEATHER_OUT  = f"gs://{GCS_BUCKET}/{PROCESSED_PREFIX}/weather/"

# ----------------------------
# Spark session
# - On Dataproc, GCS connector is built-in.
# - On a GCP VM, ensure the GCS Hadoop connector is available.
# ----------------------------
spark = (
    SparkSession.builder
    .appName("cycling-data-transformer")
    .getOrCreate()
)

# ============================
# 1) Stations
# ============================
df_stations = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(STATIONS_IN)
)

# Rename columns to a clean schema
stations = (
    df_stations
    .withColumnRenamed("Station.Id", "station_id")
    .withColumnRenamed("StationName", "station_name")
    # keep easting/northing as-is; rename if needed
)

# (Optional) quick null check per column
exprs = []
for c in stations.columns:
    # below if F.when(F.col(c).isNull() | F.isnan(c), 1).otherwise(0) return 1 else 0
    expr = F.sum(F.when(F.col(c).isNull() | F.isnan(c), 1).otherwise(0)).alias(c)
    exprs.append(expr)

stations.select(exprs).show(truncate=False)


# Write Parquet (overwrite for idempotency)
(
    stations
    .repartition(4)
    .write
    .mode("overwrite")
    .parquet(STATIONS_OUT)
)

# ============================
# 2) Weather
# Assumes your DAG preprocessed weather.json to an ARRAY of daily objects.
# ============================
df_weather = spark.read.json(WEATHER_IN)

# Drop unneeded columns (keep what you actually need)
drop_cols = [
    "cloudcover","conditions","datetimeEpoch","description","dew","icon",
    "precipcover","preciptype","source","stations","sunriseEpoch","sunsetEpoch"
]
existing_to_drop = [c for c in drop_cols if c in df_weather.columns]
weather = df_weather.drop(*existing_to_drop)

# Convert datetime -> date
if "datetime" in weather.columns:
    weather = (
        weather
        .withColumnRenamed("datetime", "weather_date")
        .withColumn("weather_date", F.col("weather_date").cast(T.DateType()))
    )

# Drop very sparse columns if present (>70% missing) — here we hard-drop a few known sparse ones
for maybe_sparse in ["precipprob", "snow", "snowdepth", "severerisk"]:
    if maybe_sparse in weather.columns:
        weather = weather.drop(maybe_sparse)



# (Optional) quick null check (excluding date key)
# Exclude the "weather_date" column
cols = [c for c in weather.columns if c != "weather_date"]

# Build expressions one by one
exprs = []
for c in cols:
    # Check if value is NULL or NaN → if yes, count it as 1, else 0
    expr = F.sum(F.when(F.col(c).isNull() | F.isnan(c), 1).otherwise(0)).alias(c)
    exprs.append(expr)

# Run the select with all those expressions
weather.select(exprs).show(truncate=False)

# Write Parquet
(
    weather
    .repartition(10)
    .write
    .mode("overwrite")
    .parquet(WEATHER_OUT)
)

spark.stop()