"""
TBD Phase 2 Benchmark Script — Group 8: Public Transport Events
Runs on: GitHub Actions runner (Python 3.11, Java 17 available)
Saves results to: benchmark_results.json
"""
import gc
import json
import os
import platform
import time
from pathlib import Path
import numpy as np
import pandas as pd
import polars as pl
import duckdb
import psutil

# ── Config ──────────────────────────────────────────────────────────────────
GROUP_ID   = 8
N_ROWS     = 10_000_000   # medium scale
OUTPUT_DIR = Path("data/phase2_26L/group_08")
EVENTS_PATH           = OUTPUT_DIR / "events.parquet"
DIMENSION_PATH        = OUTPUT_DIR / "dimension.parquet"
OPTIMIZED_EVENTS_PATH = OUTPUT_DIR / "events_optimized.parquet"
CSV_EVENTS_PATH       = OUTPUT_DIR / "events_q1.csv"
SINK_PATH             = OUTPUT_DIR / "sink_output.parquet"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

results = []

def bench(func, n=3, label=""):
    times = []
    result = None
    for i in range(n):
        gc.collect()
        t = time.perf_counter()
        result = func()
        elapsed = time.perf_counter() - t
        times.append(elapsed)
    med = round(float(np.median(times)), 4)
    print(f"  {label:40s} {med:.4f}s")
    return med, result


# ── Environment ──────────────────────────────────────────────────────────────
print("=" * 60)
print("Environment")
print("=" * 60)
print(f"Python:  {platform.python_version()}")
print(f"Pandas:  {pd.__version__}")
print(f"Polars:  {pl.__version__}")
print(f"DuckDB:  {duckdb.__version__}")
print(f"CPUs:    {psutil.cpu_count(logical=True)}")
print(f"RAM GiB: {psutil.virtual_memory().total / 2**30:.1f}")


# ── Data Generation ──────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print(f"Generating {N_ROWS:,} rows (Group 8 — Public Transport)")
print("=" * 60)
rng = np.random.default_rng(42)
t0  = time.perf_counter()
n   = N_ROWS

start = np.datetime64("2026-01-01T00:00:00", "s")
end   = np.datetime64("2026-04-01T00:00:00", "s")
secs  = int((end - start) / np.timedelta64(1, "s"))
event_ts = (start + rng.integers(0, secs, size=n).astype("timedelta64[s]")).astype("datetime64[us]")

hot = max(1, int(200_000 * 0.02))
vehicle_ids = rng.integers(hot + 1, 200_001, size=n)
mask = rng.random(n) < 0.5
vehicle_ids[mask] = rng.integers(1, hot + 1, size=mask.sum())

route_ids_vocab = np.array([f"L{i}" for i in range(1, 501)])
route_id_col    = rng.choice(route_ids_vocab, size=n)
stop_ids        = rng.integers(1, 2001, size=n)
delay_minutes   = (rng.lognormal(mean=1.0, sigma=1.2, size=n) - 2.0).round(2)
route_types     = rng.choice(["bus","tram","metro","rail","ferry"], size=n,
                              p=[0.45, 0.30, 0.15, 0.07, 0.03])
is_cancelled    = rng.random(n) < 0.02
occupancy       = rng.beta(2, 5, size=n).round(3)
passengers      = rng.integers(0, 201, size=n)
countries       = rng.choice(["PL","DE","FR","UK","US"], size=n,
                              p=[0.60, 0.15, 0.10, 0.10, 0.05])

df = pl.DataFrame({
    "event_id":       np.arange(1, n + 1),
    "vehicle_id":     vehicle_ids,
    "event_ts":       event_ts,
    "route_id":       route_id_col,
    "stop_id":        stop_ids,
    "route_type":     route_types,
    "country":        countries,
    "vehicle_type":   route_types,
    "delay_minutes":  delay_minutes,
    "is_cancelled":   is_cancelled,
    "occupancy_rate": occupancy,
    "passenger_count":passengers,
})
df = df.with_columns(pl.col("event_ts").dt.date().alias("event_date"))
df.write_parquet(EVENTS_PATH, compression="zstd")

# Dimension table
dim = pl.DataFrame({
    "route_id":       route_ids_vocab.tolist(),
    "operator":       rng.choice(["MPK","SKM","ZTM","KZK-GOP","MZK"], size=500).tolist(),
    "line_length_km": rng.uniform(2, 50, size=500).round(1).tolist(),
    "is_express":     (rng.random(500) < 0.2).tolist(),
})
dim.write_parquet(DIMENSION_PATH, compression="zstd")

# Optimized layout
df.sort(["route_type","delay_minutes"]).write_parquet(
    OPTIMIZED_EVENTS_PATH, compression="zstd", row_group_size=50_000)

# CSV for Q1 baseline
df.select(["route_id","route_type","delay_minutes"]).write_csv(CSV_EVENTS_PATH)

gen_time = round(time.perf_counter() - t0, 2)
print(f"Generated in {gen_time}s")
print(f"events.parquet:    {EVENTS_PATH.stat().st_size / 2**20:.1f} MB")
print(f"optimized.parquet: {OPTIMIZED_EVENTS_PATH.stat().st_size / 2**20:.1f} MB")
print(f"events_q1.csv:     {CSV_EVENTS_PATH.stat().st_size / 2**20:.1f} MB")


# ── Q1: Selective filter + top-50 group-by ────────────────────────────────────
print("\n" + "=" * 60)
print("Q1: Delay analysis — bus+tram, delay>5 min, top-50 routes")
print("=" * 60)

t, r = bench(lambda: (
    pd.read_parquet(EVENTS_PATH)[
        lambda d: d["route_type"].isin(["bus","tram"]) & (d["delay_minutes"] > 5)
    ].groupby("route_id")["delay_minutes"].agg(["mean","max","count"])
    .sort_values("mean", ascending=False).head(50)
), label="Pandas default")
results.append({"engine":"Pandas default","query":"Q1","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: (
    pd.read_parquet(EVENTS_PATH, engine="pyarrow", dtype_backend="pyarrow")[
        lambda d: d["route_type"].isin(["bus","tram"]) & (d["delay_minutes"] > 5)
    ].groupby("route_id")["delay_minutes"].agg(["mean","max","count"])
    .sort_values("mean", ascending=False).head(50)
), label="Pandas PyArrow backend")
results.append({"engine":"Pandas PyArrow","query":"Q1","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: (
    pl.read_parquet(EVENTS_PATH)
    .filter(pl.col("route_type").is_in(["bus","tram"]) & (pl.col("delay_minutes") > 5))
    .group_by("route_id").agg([pl.col("delay_minutes").mean().alias("avg_delay"),
                                pl.col("delay_minutes").max().alias("max_delay"),
                                pl.len().alias("cnt")])
    .sort("avg_delay", descending=True).head(50)
), label="Polars eager")
results.append({"engine":"Polars eager","query":"Q1","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: (
    pl.scan_parquet(EVENTS_PATH)
    .filter(pl.col("route_type").is_in(["bus","tram"]) & (pl.col("delay_minutes") > 5))
    .group_by("route_id").agg([pl.col("delay_minutes").mean().alias("avg_delay"),
                                pl.col("delay_minutes").max().alias("max_delay"),
                                pl.len().alias("cnt")])
    .sort("avg_delay", descending=True).head(50).collect()
), label="Polars lazy collect")
results.append({"engine":"Polars lazy","query":"Q1","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: (
    pl.scan_parquet(EVENTS_PATH)
    .filter(pl.col("route_type").is_in(["bus","tram"]) & (pl.col("delay_minutes") > 5))
    .group_by("route_id").agg([pl.col("delay_minutes").mean().alias("avg_delay"),
                                pl.col("delay_minutes").max().alias("max_delay"),
                                pl.len().alias("cnt")])
    .sort("avg_delay", descending=True).head(50).collect(engine="streaming")
), label="Polars streaming collect")
results.append({"engine":"Polars streaming","query":"Q1","median_time_s":t,"rows":len(r)})

con = duckdb.connect()
t, r = bench(lambda: con.execute(f"""
    SELECT route_id,
           AVG(delay_minutes)  AS avg_delay,
           MAX(delay_minutes)  AS max_delay,
           COUNT(*)            AS cnt
    FROM read_parquet('{EVENTS_PATH}')
    WHERE route_type IN ('bus','tram') AND delay_minutes > 5
    GROUP BY route_id
    ORDER BY avg_delay DESC
    LIMIT 50
""").df(), label="DuckDB")
results.append({"engine":"DuckDB","query":"Q1","median_time_s":t,"rows":len(r)})


# ── Q2: Hourly delay trend ────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("Q2: Hourly delay trend — route_type × hour_of_day")
print("=" * 60)

t, r = bench(lambda: (
    pd.read_parquet(EVENTS_PATH)
    .assign(hour=lambda d: pd.to_datetime(d["event_ts"]).dt.hour)
    .groupby(["route_type","hour"])["delay_minutes"]
    .agg(avg_delay="mean", p95=lambda x: x.quantile(0.95)).reset_index()
), label="Pandas default")
results.append({"engine":"Pandas default","query":"Q2","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: (
    pd.read_parquet(EVENTS_PATH, engine="pyarrow", dtype_backend="pyarrow")
    .assign(hour=lambda d: pd.to_datetime(d["event_ts"]).dt.hour)
    .groupby(["route_type","hour"])["delay_minutes"]
    .agg(avg_delay="mean", p95=lambda x: x.quantile(0.95)).reset_index()
), label="Pandas PyArrow backend")
results.append({"engine":"Pandas PyArrow","query":"Q2","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: (
    pl.scan_parquet(EVENTS_PATH)
    .with_columns(pl.col("event_ts").dt.hour().alias("hour"))
    .group_by(["route_type","hour"]).agg([
        pl.col("delay_minutes").mean().alias("avg_delay"),
        pl.col("delay_minutes").quantile(0.95).alias("p95_delay"),
        pl.col("is_cancelled").sum().alias("total_cancelled")])
    .sort(["route_type","hour"]).collect()
), label="Polars lazy")
results.append({"engine":"Polars lazy","query":"Q2","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: con.execute(f"""
    SELECT route_type,
           datepart('hour', event_ts::TIMESTAMP) AS hr,
           AVG(delay_minutes)                    AS avg_delay,
           quantile_cont(delay_minutes, 0.95)    AS p95_delay,
           SUM(CAST(is_cancelled AS INT))         AS total_cancelled
    FROM read_parquet('{EVENTS_PATH}')
    GROUP BY route_type, datepart('hour', event_ts::TIMESTAMP)
    ORDER BY route_type, hr
""").df(), label="DuckDB")
results.append({"engine":"DuckDB","query":"Q2","median_time_s":t,"rows":len(r)})


# ── Q3: Join + high-cardinality group-by ─────────────────────────────────────
print("\n" + "=" * 60)
print("Q3: Operator join — route_type × operator × is_express")
print("=" * 60)

t, r = bench(lambda: (
    pd.read_parquet(EVENTS_PATH)
    .merge(pd.read_parquet(DIMENSION_PATH), on="route_id", how="left")
    .groupby(["operator","route_type","is_express"])
    .agg(avg_occ=("occupancy_rate","mean"),
         total_pass=("passenger_count","sum"),
         avg_delay=("delay_minutes","mean"),
         cnt=("event_id","count")).reset_index()
), label="Pandas default")
results.append({"engine":"Pandas default","query":"Q3","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: (
    pd.read_parquet(EVENTS_PATH, engine="pyarrow", dtype_backend="pyarrow")
    .merge(pd.read_parquet(DIMENSION_PATH, engine="pyarrow", dtype_backend="pyarrow"),
           on="route_id", how="left")
    .groupby(["operator","route_type","is_express"])
    .agg(avg_occ=("occupancy_rate","mean"),
         total_pass=("passenger_count","sum"),
         avg_delay=("delay_minutes","mean"),
         cnt=("event_id","count")).reset_index()
), label="Pandas PyArrow backend")
results.append({"engine":"Pandas PyArrow","query":"Q3","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: (
    pl.scan_parquet(EVENTS_PATH)
    .join(pl.scan_parquet(DIMENSION_PATH), on="route_id", how="left")
    .group_by(["operator","route_type","is_express"]).agg([
        pl.col("occupancy_rate").mean().alias("avg_occ"),
        pl.col("passenger_count").sum().alias("total_pass"),
        pl.col("delay_minutes").mean().alias("avg_delay"),
        pl.len().alias("cnt")]).collect()
), label="Polars lazy")
results.append({"engine":"Polars lazy","query":"Q3","median_time_s":t,"rows":len(r)})

t, r = bench(lambda: con.execute(f"""
    SELECT e.route_type, d.operator, d.is_express,
           AVG(e.occupancy_rate)  AS avg_occ,
           SUM(e.passenger_count) AS total_pass,
           AVG(e.delay_minutes)   AS avg_delay,
           COUNT(*)               AS cnt
    FROM read_parquet('{EVENTS_PATH}') e
    LEFT JOIN read_parquet('{DIMENSION_PATH}') d USING(route_id)
    GROUP BY e.route_type, d.operator, d.is_express
    ORDER BY e.route_type, d.operator
""").df(), label="DuckDB")
results.append({"engine":"DuckDB","query":"Q3","median_time_s":t,"rows":len(r)})


# ── PySpark (optional — only if Java 17+ available) ──────────────────────────
import subprocess, sys

java_ok = False
try:
    java_ver = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode()
    print(f"\nJava: {java_ver.splitlines()[0]}")
    if "17" in java_ver or "21" in java_ver:
        java_ok = True
except Exception:
    print("\nJava not found — skipping PySpark benchmark")

if java_ok:
    print("\n" + "=" * 60)
    print("PySpark local[*]")
    print("=" * 60)
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder
             .appName("TBDPhase2")
             .master("local[*]")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    for label, func in [
        ("PySpark Q1 local[*]", lambda: (
            spark.read.parquet(str(EVENTS_PATH))
            .filter(F.col("route_type").isin("bus","tram") & (F.col("delay_minutes") > 5))
            .groupBy("route_id")
            .agg(F.avg("delay_minutes").alias("avg_delay"),
                 F.max("delay_minutes").alias("max_delay"),
                 F.count("*").alias("cnt"))
            .orderBy(F.col("avg_delay").desc()).limit(50).collect()
        )),
        ("PySpark Q2 local[*]", lambda: (
            spark.read.parquet(str(EVENTS_PATH))
            .withColumn("hr", F.hour("event_ts"))
            .groupBy("route_type","hr")
            .agg(F.avg("delay_minutes").alias("avg_delay"),
                 F.sum(F.col("is_cancelled").cast("int")).alias("cancelled"))
            .orderBy("route_type","hr").collect()
        )),
        ("PySpark Q3 local[*]", lambda: (
            spark.read.parquet(str(EVENTS_PATH))
            .join(spark.read.parquet(str(DIMENSION_PATH)), "route_id", "left")
            .groupBy("operator","route_type","is_express")
            .agg(F.avg("occupancy_rate").alias("avg_occ"),
                 F.sum("passenger_count").alias("total_pass"),
                 F.avg("delay_minutes").alias("avg_delay"),
                 F.count("*").alias("cnt"))
            .collect()
        )),
    ]:
        t, r = bench(func, n=2, label=label)
        q = label.split()[1]
        results.append({"engine":"PySpark local[*]","query":q,"median_time_s":t})

    spark.stop()
else:
    print("\nPySpark skipped (Java 17+ not available)")
    for q in ["Q1","Q2","Q3"]:
        results.append({"engine":"PySpark local[*]","query":q,"median_time_s":None,
                         "notes":"Java 17+ required"})


# ── Task 2.5: File format comparison ─────────────────────────────────────────
print("\n" + "=" * 60)
print("Task 2.5: File format / Parquet layout comparison (Q1)")
print("=" * 60)
q1_sql = ("SELECT route_id, AVG(delay_minutes) avg_delay, MAX(delay_minutes) max_delay, "
          "COUNT(*) cnt FROM read_parquet('{p}') "
          "WHERE route_type IN ('bus','tram') AND delay_minutes > 5 "
          "GROUP BY route_id ORDER BY avg_delay DESC LIMIT 50")

t_def, _ = bench(lambda: con.execute(q1_sql.format(p=EVENTS_PATH)).df(), label="Default Parquet")
t_opt, _ = bench(lambda: con.execute(q1_sql.format(p=OPTIMIZED_EVENTS_PATH)).df(), label="Optimized Parquet")
t_csv, _ = bench(lambda: con.execute(
    "SELECT route_id, AVG(delay_minutes) avg_delay, MAX(delay_minutes) max_delay, COUNT(*) cnt "
    f"FROM read_csv('{CSV_EVENTS_PATH}', AUTO_DETECT=TRUE) "
    "WHERE route_type IN ('bus','tram') AND delay_minutes > 5 "
    "GROUP BY route_id ORDER BY avg_delay DESC LIMIT 50").df(), label="CSV baseline")

results.append({"engine":"DuckDB","query":"Q1-default-parquet","median_time_s":t_def,
                "size_mb": round(EVENTS_PATH.stat().st_size/2**20, 1)})
results.append({"engine":"DuckDB","query":"Q1-optimized-parquet","median_time_s":t_opt,
                "size_mb": round(OPTIMIZED_EVENTS_PATH.stat().st_size/2**20, 1)})
results.append({"engine":"DuckDB","query":"Q1-csv","median_time_s":t_csv,
                "size_mb": round(CSV_EVENTS_PATH.stat().st_size/2**20, 1)})
print(f"  Speedup optimized vs default: {t_def/t_opt:.1f}×")
print(f"  Speedup default parquet vs CSV: {t_csv/t_def:.1f}×")


# ── Task 3.1: Polars execution modes ─────────────────────────────────────────
print("\n" + "=" * 60)
print("Task 3.1: Polars execution modes (filter delay > 1 min)")
print("=" * 60)

t_e, r = bench(lambda: pl.read_parquet(EVENTS_PATH).filter(pl.col("delay_minutes") > 1.0),
               label="Eager collect")
t_l, r = bench(lambda: pl.scan_parquet(EVENTS_PATH).filter(pl.col("delay_minutes") > 1.0).collect(),
               label="Lazy collect")
t_s, r = bench(lambda: pl.scan_parquet(EVENTS_PATH).filter(pl.col("delay_minutes") > 1.0)
               .collect(engine="streaming"), label="Streaming collect")
print(f"  Output rows: {r.height:,}")

gc.collect()
t0 = time.perf_counter()
pl.scan_parquet(EVENTS_PATH).filter(pl.col("delay_minutes") > 1.0).sink_parquet(str(SINK_PATH))
t_sink = round(time.perf_counter() - t0, 4)
print(f"  {'Sink parquet':40s} {t_sink:.4f}s  ({SINK_PATH.stat().st_size/2**20:.1f} MB on disk)")

results.append({"engine":"Polars eager","query":"mode_comparison","median_time_s":t_e})
results.append({"engine":"Polars lazy","query":"mode_comparison","median_time_s":t_l})
results.append({"engine":"Polars streaming","query":"mode_comparison","median_time_s":t_s})
results.append({"engine":"Polars sink","query":"mode_comparison","median_time_s":t_sink})


# ── Task 4: Thread scalability ────────────────────────────────────────────────
print("\n" + "=" * 60)
print("Task 4: DuckDB thread scalability")
print("=" * 60)
for thr in [1, 2, 4, psutil.cpu_count(logical=True)]:
    c = duckdb.connect()
    c.execute(f"SET threads={thr}")
    t, _ = bench(lambda c=c: c.execute(q1_sql.format(p=EVENTS_PATH)).df(), n=3,
                 label=f"DuckDB threads={thr}")
    results.append({"engine":f"DuckDB threads={thr}","query":"Q1","median_time_s":t})


# ── Save results ──────────────────────────────────────────────────────────────
out = {
    "environment": {
        "python":  platform.python_version(),
        "pandas":  pd.__version__,
        "polars":  pl.__version__,
        "duckdb":  duckdb.__version__,
        "cpu_logical_cores": psutil.cpu_count(logical=True),
        "ram_gib": round(psutil.virtual_memory().total / 2**30, 2),
        "n_rows":  N_ROWS,
        "events_parquet_mb": round(EVENTS_PATH.stat().st_size / 2**20, 1),
    },
    "results": results,
}

with open("benchmark_results.json", "w") as f:
    json.dump(out, f, indent=2)

print("\n" + "=" * 60)
print("benchmark_results.json saved")
print("=" * 60)
print(json.dumps(out, indent=2))
