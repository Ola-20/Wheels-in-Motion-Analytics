#!/usr/bin/env python3
# Transform rental journey data (aligned with proc_1_spark_dataproc_serverless_dag)

import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

# ----------------------------
# Helpers
# ----------------------------
def gs_uri(bucket: str, prefix: str) -> str:
    return f"gs://{bucket}/{prefix.lstrip('/')}"

def is_glob(p: str) -> bool:
    return any(ch in p for ch in "*?[]")

# ----------------------------
# Args & env fallbacks (match proc_1)
# ----------------------------
env = os.environ

parser = argparse.ArgumentParser()
parser.add_argument("--gcs-bucket",     default=env.get("GCS_BUCKET", "bicycle-renting-proc-analytics-bucket-12345"))
parser.add_argument("--input-prefix",   default=env.get("JOURNEY_INPUT_PREFIX", "raw/cycling-journey/*/*.csv"))
parser.add_argument("--processed-dim",  default=env.get("PROCESSED_DIM", "processed/cycling-dimension"))
parser.add_argument("--processed-fact", default=env.get("PROCESSED_FACT", "processed/cycling-fact"))
args = parser.parse_args()

GCS_BUCKET      = args.gcs_bucket
INPUT_PREFIX    = args.input_prefix          # e.g. "raw/cycling-journey/*/*.csv" OR "raw/cycling-journey"
PROCESSED_DIM   = args.processed_dim         # e.g. "processed/cycling-dimension"
PROCESSED_FACT  = args.processed_fact        # e.g. "processed/cycling-fact"

JOURNEY_IN   = gs_uri(GCS_BUCKET, INPUT_PREFIX)
STATIONS_DIM = gs_uri(GCS_BUCKET, f"{PROCESSED_DIM.rstrip('/')}/stations/")
DATETIME_OUT = gs_uri(GCS_BUCKET, f"{PROCESSED_DIM.rstrip('/')}/datetime/")
JOURNEY_OUT  = gs_uri(GCS_BUCKET, f"{PROCESSED_FACT.rstrip('/')}/journey/")

# ----------------------------
# Spark session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("journey-transform-gcs")
    .getOrCreate()
)

# ============================
# 1) Load journey CSVs
# ============================
reader = spark.read.option("header", "true").option("inferSchema", "true")
if is_glob(INPUT_PREFIX):
    df_journey = reader.csv(JOURNEY_IN)
else:
    # If a directory/root is passed, read recursively
    df_journey = reader.option("recursiveFileLookup", "true").csv(JOURNEY_IN)

# ============================
# 2) (Optional) normalize a few known variants for timestamps only
#    We keep station columns AS-IS and will reference them with backticks in SQL.
# ============================
rename_map = {
    "Start Date": "start_date",
    "Start date": "start_date",
    "End Date":   "end_date",
    "End date":   "end_date",
}
for src, dst in rename_map.items():
    if src in df_journey.columns:
        df_journey = df_journey.withColumnRenamed(src, dst)

# ============================
# 3) Parse timestamps + weather_date
# ============================
time_fmt = "dd/MM/yyyy HH:mm"
if "start_date" in df_journey.columns:
    df_journey = df_journey.withColumn("start_date", F.to_timestamp(F.col("start_date"), time_fmt))
if "end_date" in df_journey.columns:
    df_journey = df_journey.withColumn("end_date",   F.to_timestamp(F.col("end_date"),   time_fmt))
if "start_date" in df_journey.columns:
    df_journey = df_journey.withColumn("weather_date", F.to_date(F.col("start_date")))

# ============================
# 4) Build/refresh stations dim with INT ids (overwrite)
#    Use exact CSV headers with backticks; cast IDs to BIGINT so Parquet → BQ INT64 is clean.
# ============================
station_schema = T.StructType([
    T.StructField("station_id",   T.LongType(),  True),  # INT64 in BQ
    T.StructField("station_name", T.StringType(), True),
    T.StructField("longitude",    T.DoubleType(), True),
    T.StructField("latitude",     T.DoubleType(), True),
    T.StructField("easting",      T.DoubleType(), True),
    T.StructField("northing",     T.DoubleType(), True),
])

# Read any existing stations and coerce to LONG for a clean union
try:
    df_station_dim = spark.read.parquet(STATIONS_DIM)
    if "station_id" in df_station_dim.columns:
        df_station_dim = df_station_dim.withColumn("station_id", F.col("station_id").cast(T.LongType()))
    else:
        df_station_dim = spark.createDataFrame(spark.sparkContext.emptyRDD(), station_schema)
except Exception:
    df_station_dim = spark.createDataFrame(spark.sparkContext.emptyRDD(), station_schema)

df_journey.createOrReplaceTempView("journey")
df_station_dim.createOrReplaceTempView("station")

additional_stations = spark.sql("""
WITH station_ids AS (
  SELECT station_id FROM station
)
SELECT DISTINCT
  CAST(j.`Start station number` AS BIGINT) AS station_id,
  j.`Start station`                        AS station_name
FROM journey j
WHERE j.`Start station number` IS NOT NULL
  AND CAST(j.`Start station number` AS BIGINT) NOT IN (SELECT station_id FROM station_ids)
UNION
SELECT DISTINCT
  CAST(j.`End station number`   AS BIGINT) AS station_id,
  j.`End station`                          AS station_name
FROM journey j
WHERE j.`End station number` IS NOT NULL
  AND CAST(j.`End station number` AS BIGINT) NOT IN (SELECT station_id FROM station_ids)
""")

additional_stations = (
    additional_stations
    .withColumn("longitude", F.lit(0.0).cast(T.DoubleType()))
    .withColumn("latitude",  F.lit(0.0).cast(T.DoubleType()))
    .withColumn("easting",   F.lit(0.0).cast(T.DoubleType()))
    .withColumn("northing",  F.lit(0.0).cast(T.DoubleType()))
)

# Union with existing (both are LONG now), then overwrite folder to fix schema
df_station_all = (
    df_station_dim.select("station_id", "station_name", "longitude", "latitude", "easting", "northing")
    .unionByName(additional_stations.select("station_id", "station_name", "longitude", "latitude", "easting", "northing"), allowMissingColumns=True)
    .dropDuplicates(["station_id"])
)

# Overwrite to ensure a single consistent schema on disk
df_station_all.write.mode("overwrite").parquet(STATIONS_DIM)

# ============================
# 5) Drop journey columns no longer needed (use exact names)
# ============================
cols_to_drop = ["Start station", "End station", "Total duration", "Total duration (ms)"]
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

dfs = []
if "start_date" in df_journey.columns:
    dfs.append(build_datetime_df(df_journey, "start_date"))
if "end_date" in df_journey.columns:
    dfs.append(build_datetime_df(df_journey, "end_date"))

if dfs:
    df_datetime = dfs[0]
    for d in dfs[1:]:
        df_datetime = df_datetime.unionByName(d).dropDuplicates(["datetime_id"])
else:
    df_datetime = spark.createDataFrame([], T.StructType([
        T.StructField("datetime_id", T.TimestampType(), True),
        T.StructField("year",        T.IntegerType(),   True),
        T.StructField("week_day",    T.IntegerType(),   True),
        T.StructField("month",       T.IntegerType(),   True),
        T.StructField("day",         T.IntegerType(),   True),
        T.StructField("hour",        T.IntegerType(),   True),
        T.StructField("minute",      T.IntegerType(),   True),
        T.StructField("second",      T.IntegerType(),   True),
    ]))

# ============================
# 7) Write outputs (Parquet)
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
