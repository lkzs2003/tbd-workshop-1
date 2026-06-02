import json
import pathlib

bench_file = pathlib.Path("benchmark_results.json")
if not bench_file.exists():
    print("ERROR: benchmark_results.json not found")
    exit(1)

data = json.loads(bench_file.read_text())

try:
    dp = json.loads(pathlib.Path("dataproc_results.json").read_text())
    for q, t in dp.items():
        data["results"].append({
            "engine": "PySpark Dataproc",
            "query": q,
            "median_time_s": t
        })
    print(f"Merged {len(dp)} Dataproc results")
except Exception as e:
    print(f"No Dataproc results: {e}")

pathlib.Path("benchmark_results_final.json").write_text(json.dumps(data, indent=2))
print(json.dumps(data, indent=2))
