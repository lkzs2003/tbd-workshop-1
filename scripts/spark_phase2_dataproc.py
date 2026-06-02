import time
import json
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TBDPhase2Dataproc").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BUCKET = "gs://tbd-2026l-325072-code"
EVENTS = BUCKET + "/phase2/group_08/events.parquet"
DIM    = BUCKET + "/phase2/group_08/dimension.parquet"


def bench(func, n=2):
    times = []
    for _ in range(n):
        t = time.perf_counter()
        func()
        times.append(time.perf_counter() - t)
    return round(min(times), 3)


results = {}

results["Q1"] = bench(lambda: spark.read.parquet(EVENTS)
    .filter(F.col("route_type").isin("bus", "tram") & (F.col("delay_minutes") > 5))
    .groupBy("route_id")
    .agg(F.avg("delay_minutes").alias("avg"), F.count("*").alias("cnt"))
    .orderBy(F.col("avg").desc()).limit(50).collect())

results["Q2"] = bench(lambda: spark.read.parquet(EVENTS)
    .withColumn("hr", F.hour("event_ts"))
    .groupBy("route_type", "hr")
    .agg(F.avg("delay_minutes").alias("avg_delay"),
         F.sum(F.col("is_cancelled").cast("int")).alias("cancelled"))
    .orderBy("route_type", "hr").collect())

results["Q3"] = bench(lambda: spark.read.parquet(EVENTS)
    .join(spark.read.parquet(DIM), "route_id", "left")
    .groupBy("operator", "route_type", "is_express")
    .agg(F.avg("delay_minutes").alias("avg_delay"), F.count("*").alias("cnt"))
    .collect())

print("DATAPROC_RESULTS:" + json.dumps(results))
spark.stop()
