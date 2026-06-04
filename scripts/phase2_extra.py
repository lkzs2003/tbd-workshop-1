"""
TBD Phase 2 — polish run: fills the last small gaps with REAL measurements.
  * Polars eager / lazy / streaming for Q2 and Q3 (Task 2 asked for all three modes).
  * PySpark local Q1/Q2/Q3 at n=5 (was n=2 — below the "at least 3" rule) and
    local[1] / local[2] / local[4] for Q1 (Task 4 scalability), one coherent run so
    every Spark number stays tied together.
Runs on the CI runner (Python 3.11, Java 17). No GCP. Prints an EXTRAJSON blob.
"""
import time, gc, json, statistics
from pathlib import Path
import numpy as np
import polars as pl

OUT = Path("data/phase2_extra"); OUT.mkdir(parents=True, exist_ok=True)
EVENTS = OUT / "events.parquet"; DIM = OUT / "dimension.parquet"
N_ROWS = 2_000_000


def generate():
    if EVENTS.exists() and DIM.exists():
        return
    rng = np.random.default_rng(42)
    n = N_ROWS
    start = np.datetime64("2026-01-01T00:00:00", "s"); end = np.datetime64("2026-04-01T00:00:00", "s")
    secs = int((end - start) / np.timedelta64(1, "s"))
    event_ts = (start + rng.integers(0, secs, size=n).astype("timedelta64[s]")).astype("datetime64[us]")
    hot = max(1, int(200_000 * 0.02))
    vid = rng.integers(hot + 1, 200_001, size=n); m = rng.random(n) < 0.5
    vid[m] = rng.integers(1, hot + 1, size=m.sum())
    rv = np.array([f"L{i}" for i in range(1, 501)])
    rt = rng.choice(["bus", "tram", "metro", "rail", "ferry"], size=n, p=[0.45, 0.30, 0.15, 0.07, 0.03])
    pl.DataFrame({
        "event_id": np.arange(1, n + 1), "vehicle_id": vid, "event_ts": event_ts,
        "route_id": rng.choice(rv, size=n), "stop_id": rng.integers(1, 2001, size=n),
        "route_type": rt, "country": rng.choice(["PL", "DE", "FR", "UK", "US"], size=n, p=[0.6, 0.15, 0.1, 0.1, 0.05]),
        "vehicle_type": rt, "delay_minutes": (rng.lognormal(1.0, 1.2, size=n) - 2.0).round(2),
        "is_cancelled": rng.random(n) < 0.02, "occupancy_rate": rng.beta(2, 5, size=n).round(3),
        "passenger_count": rng.integers(0, 201, size=n),
    }).with_columns(pl.col("event_ts").dt.date().alias("event_date")).write_parquet(EVENTS, compression="zstd")
    pl.DataFrame({"route_id": rv.tolist(), "operator": rng.choice(["MPK", "SKM", "ZTM", "KZK-GOP", "MZK"], size=500).tolist(),
                  "line_length_km": rng.uniform(2, 50, size=500).round(1).tolist(),
                  "is_express": (rng.random(500) < 0.2).tolist()}).write_parquet(DIM, compression="zstd")


def med(func, n):
    ts = []
    for _ in range(n):
        gc.collect(); t = time.perf_counter(); func(); ts.append(time.perf_counter() - t)
    return round(float(statistics.median(ts)), 4)


generate()
res = {}

# ── Polars eager / lazy / streaming for Q2 and Q3 ────────────────────────────
def pl_q2_eager():
    return (pl.read_parquet(EVENTS).with_columns(pl.col("event_ts").dt.hour().alias("hour"))
            .group_by(["route_type", "hour"]).agg([pl.col("delay_minutes").mean().alias("avg_delay"),
            pl.col("delay_minutes").quantile(0.95).alias("p95"), pl.col("is_cancelled").sum()]).sort(["route_type", "hour"]))
def pl_q2_lazy(stream=False):
    q = (pl.scan_parquet(EVENTS).with_columns(pl.col("event_ts").dt.hour().alias("hour"))
         .group_by(["route_type", "hour"]).agg([pl.col("delay_minutes").mean().alias("avg_delay"),
         pl.col("delay_minutes").quantile(0.95).alias("p95"), pl.col("is_cancelled").sum()]).sort(["route_type", "hour"]))
    return q.collect(engine="streaming") if stream else q.collect()
def pl_q3_eager():
    return (pl.read_parquet(EVENTS).join(pl.read_parquet(DIM), on="route_id", how="left")
            .group_by(["operator", "route_type", "is_express"]).agg([pl.col("occupancy_rate").mean(),
            pl.col("passenger_count").sum(), pl.col("delay_minutes").mean(), pl.len()]))
def pl_q3_lazy(stream=False):
    q = (pl.scan_parquet(EVENTS).join(pl.scan_parquet(DIM), on="route_id", how="left")
         .group_by(["operator", "route_type", "is_express"]).agg([pl.col("occupancy_rate").mean(),
         pl.col("passenger_count").sum(), pl.col("delay_minutes").mean(), pl.len()]))
    return q.collect(engine="streaming") if stream else q.collect()

res["polars_Q2_eager"] = med(pl_q2_eager, 5)
res["polars_Q2_lazy"] = med(lambda: pl_q2_lazy(False), 5)
res["polars_Q2_streaming"] = med(lambda: pl_q2_lazy(True), 5)
res["polars_Q3_eager"] = med(pl_q3_eager, 5)
res["polars_Q3_lazy"] = med(lambda: pl_q3_lazy(False), 5)
res["polars_Q3_streaming"] = med(lambda: pl_q3_lazy(True), 5)
print("Polars Q2/Q3 modes done")

# ── PySpark local: Q1/Q2/Q3 at n=5 on local[*], plus local[1]/[2]/[4] for Q1 ──
from pyspark.sql import SparkSession, functions as F

def spark_q1(s):
    return (s.read.parquet(str(EVENTS)).filter(F.col("route_type").isin("bus", "tram") & (F.col("delay_minutes") > 5))
            .groupBy("route_id").agg(F.avg("delay_minutes").alias("a"), F.max("delay_minutes"), F.count("*"))
            .orderBy(F.col("a").desc()).limit(50).collect())
def spark_q2(s):
    return (s.read.parquet(str(EVENTS)).withColumn("hr", F.hour("event_ts")).groupBy("route_type", "hr")
            .agg(F.avg("delay_minutes"), F.sum(F.col("is_cancelled").cast("int"))).orderBy("route_type", "hr").collect())
def spark_q3(s):
    return (s.read.parquet(str(EVENTS)).join(s.read.parquet(str(DIM)), "route_id", "left")
            .groupBy("operator", "route_type", "is_express").agg(F.avg("occupancy_rate"), F.sum("passenger_count"),
            F.avg("delay_minutes"), F.count("*")).collect())

for cores in (1, 2, 4):
    s = (SparkSession.builder.appName(f"x{cores}").master(f"local[{cores}]")
         .config("spark.driver.memory", "4g").config("spark.sql.shuffle.partitions", "8").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    res[f"spark_Q1_local{cores}"] = med(lambda s=s: spark_q1(s), 5 if cores == 4 else 3)
    if cores == 4:
        res["spark_Q2_local4"] = med(lambda s=s: spark_q2(s), 5)
        res["spark_Q3_local4"] = med(lambda s=s: spark_q3(s), 5)
    s.stop()
print("PySpark done")

print("EXTRAJSON:" + json.dumps(res))
