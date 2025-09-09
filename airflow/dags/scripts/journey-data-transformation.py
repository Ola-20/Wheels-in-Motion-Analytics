#!/usr/bin/env python3
# Transform rental journey data

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

# ----------------------------
# Config (change via env if needed)
# ----------------------------
GCS_BUCKET        = os.environ.get("GCS_BUCKET", "your-bucket-name")
RAW_JOURNEY_PREFIX = os.environ.get("RAW_JOURNEY_PREFIX", "raw/cycling-journey")
PROCESSED_DIM      = os.environ.get("PROCESSED_DIM", "processed/cycling-dimension")
PROCESSED_FACT     = os.environ.get("PROCESSED_FACT", "processed/cycling-fact")

JOURNEY_IN   = f"gs://{GCS_BUCKET}/{RAW_JOURNEY_PREFIX}/*/*"          # e.g. year/month folders
STATIONS_DIM = f"gs://{GCS_BUCKET}/{PROCESSED_DIM}/stations/"         # existing dim
DATETIME_OUT = f"gs://{GCS_BUCKET}/{PROCESSED_DIM}/datetime/"         # new dim
JOURNEY_OUT  = f"gs://{GCS_BUCKET}/{PROCESSED_FACT}/journey/"         # fact

# ----------------------------
# Spark session
# - On Dataproc, GCS connector is built-in.
# - On a GCP VM, ensure the GCS connector is available.
# ----------------------------
spark = (
    SparkSession.builder
    .appName("journey-transform-gcs")
    .getOrCreate()
)

# ============================
# 1) Load journey CSVs
# ============================
df_journey = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(JOURNEY_IN)
)

# ============================
# 2) Rename columns (lowercase, remove spaces in key fields)
#    Keep the station-name columns for now (used later), drop them after the merge step.
# ============================
df_journey = (
    df_journey
    .withColumnRenamed("Rental Id",       "rental_id")
    .withColumnRenamed("Bike Id",         "bike_id")
    .withColumnRenamed("Start Date",      "start_date")
    .withColumnRenamed("End Date",        "end_date")
    .withColumnRenamed("StartStation Id", "start_station")
    .withColumnRenamed("EndStation Id",   "end_station")
)

# ============================
# 3) Convert types (strings -> timestamps) and add weather_date (date)
#    NOTE: original format should be 'dd/MM/yyyy HH:mm' (four y's)
# ============================
time_fmt = "dd/MM/yyyy HH:mm"
df_journey = df_journey.withColumn("start_date", F.to_timestamp(F.col("start_date"), time_fmt))
df_journey = df_journey.withColumn("end_date",   F.to_timestamp(F.col("end_date"),   time_fmt))
df_journey = df_journey.withColumn("weather_date", F.to_date(F.col("start_date")))  # date part of start

# ============================
# 4) Update stations dimension with any new station IDs seen in journeys
#    - Read existing stations dim (from the first helper script output)
#    - Find station IDs in journeys that are not in the dim
#    - Build minimal rows for those (id + name + placeholder coords)
#    - Append them to the stations dim
# ============================
# Load existing stations dimension (Parquet)
df_station_dim = spark.read.parquet(STATIONS_DIM)

# Temp views for simple SQL logic (optional—could be done with joins too)
df_journey.createOrReplaceTempView("journey")
df_station_dim.createOrReplaceTempView("station")

additional_stations = spark.sql("""
with station_ids as (
  select station_id from station
)
select distinct
  cast(j.start_station as string) as station_id,
  j.`StartStation Name`           as station_name
from journey j
where j.start_station is not null
  and cast(j.start_station as string) not in (select station_id from station_ids)

union

select distinct
  cast(j.end_station as string)   as station_id,
  j.`EndStation Name`             as station_name
from journey j
where j.end_station is not null
  and cast(j.end_station as string) not in (select station_id from station_ids)
""")

# Add placeholder numeric columns expected by the dim schema (adjust as needed)
# Using zeros here—replace later if you have real geo data.
additional_stations = (
    additional_stations
    .withColumn("longitude", F.lit(0.0).cast(T.DoubleType()))
    .withColumn("latitude",  F.lit(0.0).cast(T.DoubleType()))
    .withColumn("easting",   F.lit(0.0).cast(T.DoubleType()))
    .withColumn("northing",  F.lit(0.0).cast(T.DoubleType()))
    .dropDuplicates(["station_id"]) # in case a station is in both start and end after union
)

# Append any new stations to the dim
# (If there are none, this will just write an empty job; that's fine.)
if additional_stations.limit(1).count() > 0:
    (additional_stations.write.mode("append").parquet(STATIONS_DIM))

# ============================
# 5) Drop journey columns no longer needed
# ============================
cols_to_drop = ["StartStation Name", "EndStation Name", "Duration"]
df_journey = df_journey.drop(*[c for c in cols_to_drop if c in df_journey.columns])

# ============================
# 6) Build datetime dimension (from start_date and end_date)
# ============================
def build_datetime_df(df, ts_col):
    return (
        df.select(
            F.col(ts_col).alias("datetime_id"),
            F.year(F.col(ts_col)).alias("year"),
            F.dayofweek(F.col(ts_col)).alias("week_day"),
            F.month(F.col(ts_col)).alias("month"),
            F.dayofmonth(F.col(ts_col)).alias("day"),
            F.hour(F.col(ts_col)).alias("hour"),
            F.minute(F.col(ts_col)).alias("minute"),
            F.second(F.col(ts_col)).alias("second"),
        )
        .where(F.col(ts_col).isNotNull())
    )

dt_from_start = build_datetime_df(df_journey, "start_date")
dt_from_end   = build_datetime_df(df_journey, "end_date")

df_datetime = dt_from_start.unionByName(dt_from_end).dropDuplicates(["datetime_id"])

# ============================
# 7) Write outputs (Parquet)
#    - datetime dim (append)
#    - journey fact (append)
# ============================
(
    df_datetime
    .repartition(8)
    .write
    .mode("append")
    .parquet(DATETIME_OUT)
)

(
    df_journey
    .repartition(16)
    .write
    .mode("append")
    .parquet(JOURNEY_OUT)
)

spark.stop()