"""
TBD Phase 2 — remediation script: PEAK MEMORY (fresh process) + PRUNING evidence + dtypes.

Fills the gaps the instructor explicitly asked for but the first pass missed:
  * Part 2 / Task 3.1: peak memory per engine, measured in a FRESH process
    (each variant runs in its own subprocess; the parent samples the whole
    process-tree RSS so the Spark JVM is included too).
  * Task 2.5: real pruning evidence (DuckDB EXPLAIN ANALYZE rows scanned +
    Parquet row-group metadata / min-max stats for default vs optimized layout).
  * Pandas: dtypes of default (NumPy) vs PyArrow dtype_backend.

Runs on the GitHub Actions runner (Python 3.11, Java 17). No GCP needed.

Usage:
  python scripts/phase2_memory_pruning.py            # parent: generate + measure + report
  python scripts/phase2_memory_pruning.py --child ENGINE QUERY   # internal worker
"""
import sys, os, gc, time, json, platform, subprocess
from pathlib import Path
import numpy as np
import pandas as pd
import polars as pl
import duckdb
import psutil

OUT = Path("data/phase2_mem")
OUT.mkdir(parents=True, exist_ok=True)
EVENTS = OUT / "events.parquet"
DIM    = OUT / "dimension.parquet"
OPT    = OUT / "events_optimized.parquet"
CSV    = OUT / "events_q1.csv"
SINK   = OUT / "sink_output.parquet"
N_ROWS = 2_000_000


# ── Data generation (identical schema to the main Group 8 benchmark) ──────────
def generate():
    if EVENTS.exists() and DIM.exists() and OPT.exists():
        return
    rng = np.random.default_rng(42)
    n = N_ROWS
    start = np.datetime64("2026-01-01T00:00:00", "s")
    end   = np.datetime64("2026-04-01T00:00:00", "s")
    secs  = int((end - start) / np.timedelta64(1, "s"))
    event_ts = (start + rng.integers(0, secs, size=n).astype("timedelta64[s]")).astype("datetime64[us]")
    hot = max(1, int(200_000 * 0.02))
    vehicle_ids = rng.integers(hot + 1, 200_001, size=n)
    mask = rng.random(n) < 0.5
    vehicle_ids[mask] = rng.integers(1, hot + 1, size=mask.sum())
    route_ids_vocab = np.array([f"L{i}" for i in range(1, 501)])
    route_id_col = rng.choice(route_ids_vocab, size=n)
    delay_minutes = (rng.lognormal(mean=1.0, sigma=1.2, size=n) - 2.0).round(2)
    route_types = rng.choice(["bus", "tram", "metro", "rail", "ferry"], size=n,
                             p=[0.45, 0.30, 0.15, 0.07, 0.03])
    df = pl.DataFrame({
        "event_id": np.arange(1, n + 1),
        "vehicle_id": vehicle_ids,
        "event_ts": event_ts,
        "route_id": route_id_col,
        "stop_id": rng.integers(1, 2001, size=n),
        "route_type": route_types,
        "country": rng.choice(["PL", "DE", "FR", "UK", "US"], size=n, p=[0.60, 0.15, 0.10, 0.10, 0.05]),
        "vehicle_type": route_types,
        "delay_minutes": delay_minutes,
        "is_cancelled": rng.random(n) < 0.02,
        "occupancy_rate": rng.beta(2, 5, size=n).round(3),
        "passenger_count": rng.integers(0, 201, size=n),
    }).with_columns(pl.col("event_ts").dt.date().alias("event_date"))
    df.write_parquet(EVENTS, compression="zstd")
    dim = pl.DataFrame({
        "route_id": route_ids_vocab.tolist(),
        "operator": rng.choice(["MPK", "SKM", "ZTM", "KZK-GOP", "MZK"], size=500).tolist(),
        "line_length_km": rng.uniform(2, 50, size=500).round(1).tolist(),
        "is_express": (rng.random(500) < 0.2).tolist(),
    })
    dim.write_parquet(DIM, compression="zstd")
    # optimized = sorted by the Q1 filter column, small row groups -> better pruning
    df.sort(["route_type", "delay_minutes"]).write_parquet(OPT, compression="zstd", row_group_size=50_000)
    df.select(["route_id", "route_type", "delay_minutes"]).write_csv(CSV)


# ── One query variant (kept referenced so its memory stays resident) ──────────
def run_variant(engine, query):
    if engine in ("pandas_default", "pandas_pyarrow"):
        kw = dict(engine="pyarrow", dtype_backend="pyarrow") if engine == "pandas_pyarrow" else {}
        if query == "Q1":
            d = pd.read_parquet(EVENTS, **kw)
            return (d[d["route_type"].isin(["bus", "tram"]) & (d["delay_minutes"] > 5)]
                    .groupby("route_id")["delay_minutes"].agg(["mean", "max", "count"])
                    .sort_values("mean", ascending=False).head(50))
        if query == "Q2":
            d = pd.read_parquet(EVENTS, **kw)
            d["hour"] = pd.to_datetime(d["event_ts"]).dt.hour
            return d.groupby(["route_type", "hour"])["delay_minutes"].mean().reset_index()
        if query == "Q3":
            d = pd.read_parquet(EVENTS, **kw).merge(pd.read_parquet(DIM, **kw), on="route_id", how="left")
            return (d.groupby(["operator", "route_type", "is_express"])
                    .agg(avg_occ=("occupancy_rate", "mean"), total_pass=("passenger_count", "sum"),
                         avg_delay=("delay_minutes", "mean"), cnt=("event_id", "count")).reset_index())
    if engine == "polars_lazy":
        if query == "Q1":
            return (pl.scan_parquet(EVENTS)
                    .filter(pl.col("route_type").is_in(["bus", "tram"]) & (pl.col("delay_minutes") > 5))
                    .group_by("route_id").agg(pl.col("delay_minutes").mean(), pl.len())
                    .sort("delay_minutes", descending=True).head(50).collect())
        if query == "Q2":
            return (pl.scan_parquet(EVENTS).with_columns(pl.col("event_ts").dt.hour().alias("hour"))
                    .group_by(["route_type", "hour"]).agg(pl.col("delay_minutes").mean()).collect())
        if query == "Q3":
            return (pl.scan_parquet(EVENTS).join(pl.scan_parquet(DIM), on="route_id", how="left")
                    .group_by(["operator", "route_type", "is_express"])
                    .agg(pl.col("occupancy_rate").mean(), pl.col("passenger_count").sum(), pl.len()).collect())
    if engine == "duckdb":
        c = duckdb.connect()
        sql = {
            "Q1": f"SELECT route_id, AVG(delay_minutes) a, COUNT(*) c FROM read_parquet('{EVENTS}') "
                  "WHERE route_type IN ('bus','tram') AND delay_minutes>5 GROUP BY route_id ORDER BY a DESC LIMIT 50",
            "Q2": f"SELECT route_type, datepart('hour',event_ts::TIMESTAMP) h, AVG(delay_minutes) a "
                  f"FROM read_parquet('{EVENTS}') GROUP BY route_type, h",
            "Q3": f"SELECT d.operator, e.route_type, d.is_express, AVG(e.occupancy_rate) o, COUNT(*) c "
                  f"FROM read_parquet('{EVENTS}') e LEFT JOIN read_parquet('{DIM}') d USING(route_id) "
                  "GROUP BY d.operator, e.route_type, d.is_express",
        }[query]
        return c.execute(sql).df()
    if engine == "pyspark":
        from pyspark.sql import SparkSession, functions as F
        spark = (SparkSession.builder.appName("mem").master("local[*]")
                 .config("spark.driver.memory", "3g").config("spark.sql.shuffle.partitions", "8")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        if query == "Q1":
            r = (spark.read.parquet(str(EVENTS))
                 .filter(F.col("route_type").isin("bus", "tram") & (F.col("delay_minutes") > 5))
                 .groupBy("route_id").agg(F.avg("delay_minutes").alias("a"), F.count("*"))
                 .orderBy(F.col("a").desc()).limit(50).collect())
        elif query == "Q2":
            r = (spark.read.parquet(str(EVENTS)).withColumn("h", F.hour("event_ts"))
                 .groupBy("route_type", "h").agg(F.avg("delay_minutes")).collect())
        else:
            r = (spark.read.parquet(str(EVENTS)).join(spark.read.parquet(str(DIM)), "route_id", "left")
                 .groupBy("operator", "route_type", "is_express").agg(F.avg("occupancy_rate"), F.count("*")).collect())
        time.sleep(0.3)
        spark.stop()
        return r
    # Task 3.1 modes on a large-output filter (keep ~47% rows)
    if engine == "mode_eager":
        return pl.read_parquet(EVENTS).filter(pl.col("delay_minutes") > 1.0)
    if engine == "mode_lazy":
        return pl.scan_parquet(EVENTS).filter(pl.col("delay_minutes") > 1.0).collect()
    if engine == "mode_streaming":
        return pl.scan_parquet(EVENTS).filter(pl.col("delay_minutes") > 1.0).collect(engine="streaming")
    if engine == "mode_sink":
        pl.scan_parquet(EVENTS).filter(pl.col("delay_minutes") > 1.0).sink_parquet(str(SINK))
        return None
    raise ValueError(f"unknown {engine}/{query}")


# ── Child entry point ─────────────────────────────────────────────────────────
if len(sys.argv) >= 4 and sys.argv[1] == "--child":
    generate()
    gc.collect()
    res = run_variant(sys.argv[2], sys.argv[3])
    time.sleep(0.4)          # hold peak so the parent can sample it
    del res
    sys.exit(0)


# ── Parent: peak-RSS sampler over the whole child process tree ────────────────
def tree_rss_mb(proc):
    try:
        procs = [proc] + proc.children(recursive=True)
        return sum(p.memory_info().rss for p in procs if p.is_running()) / 2**20
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return 0.0


def measure(engine, query):
    p = subprocess.Popen([sys.executable, __file__, "--child", engine, query])
    pr = psutil.Process(p.pid)
    peak = 0.0
    while p.poll() is None:
        peak = max(peak, tree_rss_mb(pr))
        time.sleep(0.005)
    return round(peak, 1)


def main():
    print("=" * 64)
    print("Environment")
    print("=" * 64)
    print(f"Python {platform.python_version()} | pandas {pd.__version__} | "
          f"polars {pl.__version__} | duckdb {duckdb.__version__}")
    print(f"CPUs {psutil.cpu_count(logical=True)} | RAM {psutil.virtual_memory().total/2**30:.1f} GiB")
    generate()

    # ── 1) Pandas dtypes: default (NumPy) vs PyArrow backend ──────────────────
    print("\n" + "=" * 64)
    print("Pandas dtypes — default (NumPy-backed) vs PyArrow dtype_backend")
    print("=" * 64)
    d_def = pd.read_parquet(EVENTS)
    d_pa  = pd.read_parquet(EVENTS, engine="pyarrow", dtype_backend="pyarrow")
    for col in d_def.columns:
        print(f"  {col:16s} default={str(d_def[col].dtype):14s} pyarrow={d_pa[col].dtype}")
    del d_def, d_pa
    gc.collect()

    # ── 2) Peak memory in fresh processes (peak tree RSS) ─────────────────────
    print("\n" + "=" * 64)
    print("Peak memory — fresh process per variant (peak process-tree RSS, MB)")
    print("=" * 64)
    matrix = [(e, q) for q in ("Q1", "Q2", "Q3")
              for e in ("pandas_default", "pandas_pyarrow", "polars_lazy", "duckdb", "pyspark")]
    mem = {}
    print(f"{'query':6}{'pandas_def':>12}{'pandas_pa':>12}{'polars_lazy':>13}{'duckdb':>9}{'pyspark':>9}")
    for q in ("Q1", "Q2", "Q3"):
        row = {}
        for e in ("pandas_default", "pandas_pyarrow", "polars_lazy", "duckdb", "pyspark"):
            row[e] = measure(e, q)
            mem[f"{e}|{q}"] = row[e]
        print(f"{q:6}{row['pandas_default']:12.1f}{row['pandas_pyarrow']:12.1f}"
              f"{row['polars_lazy']:13.1f}{row['duckdb']:9.1f}{row['pyspark']:9.1f}")

    # ── 3) Task 3.1 peak memory: eager / lazy / streaming / sink ──────────────
    print("\n" + "=" * 64)
    print("Task 3.1 peak memory (fresh process) — filter delay>1min (~47% rows kept)")
    print("=" * 64)
    for mode in ("mode_eager", "mode_lazy", "mode_streaming", "mode_sink"):
        m = measure(mode, "T31")
        mem[f"{mode}|T31"] = m
        print(f"  {mode:16s} peak_rss={m:8.1f} MB")

    # ── 4) Task 2.5 pruning evidence (DuckDB EXPLAIN ANALYZE + row-group meta) ─
    print("\n" + "=" * 64)
    print("Task 2.5 pruning evidence — DuckDB EXPLAIN ANALYZE (rows scanned)")
    print("=" * 64)
    con = duckdb.connect()
    con.execute("PRAGMA enable_profiling='no_output'")
    q1 = ("SELECT route_id, AVG(delay_minutes) a FROM read_parquet('{p}') "
          "WHERE route_type IN ('bus','tram') AND delay_minutes>5 GROUP BY route_id ORDER BY a DESC LIMIT 50")
    for label, path in [("default", EVENTS), ("optimized", OPT)]:
        plan = con.execute("EXPLAIN ANALYZE " + q1.format(p=path)).fetchall()[0][1]
        # pull the PARQUET_SCAN / cardinality lines
        scanned = [ln.strip() for ln in plan.splitlines()
                   if any(k in ln for k in ("PARQUET_SCAN", "Rows", "Total Time", "FILTER"))]
        print(f"\n  --- {label} layout ({Path(path).name}) ---")
        for ln in plan.splitlines():
            s = ln.strip()
            if s:
                print("   " + s)

    print("\n" + "-" * 64)
    print("Parquet row-group metadata (num row groups + route_type min/max per group)")
    print("-" * 64)
    for label, path in [("default", EVENTS), ("optimized", OPT)]:
        nrg = con.execute(f"SELECT COUNT(DISTINCT row_group_id) FROM parquet_metadata('{path}')").fetchone()[0]
        rows = con.execute(f"SELECT num_rows FROM parquet_file_metadata('{path}')").fetchone()[0]
        print(f"\n  {label}: {nrg} row groups, {rows:,} rows "
              f"(~{rows//max(nrg,1):,} rows/group)")
        stats = con.execute(
            f"SELECT row_group_id, stats_min_value, stats_max_value "
            f"FROM parquet_metadata('{path}') WHERE path_in_schema='route_type' "
            f"ORDER BY row_group_id LIMIT 6").fetchall()
        for rg, mn, mx in stats:
            skip = "PRUNABLE" if mn == mx else "spans many -> cannot skip"
            print(f"     row_group {rg}: route_type in [{mn}..{mx}]  ({skip})")

    # ── 5) consolidated memory dump ──────────────────────────────────────────
    print("\n" + "=" * 64)
    print("MEMJSON:" + json.dumps(mem))
    print("=" * 64)


if __name__ == "__main__":
    main()
