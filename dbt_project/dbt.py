"""ELT pipeline: load the raw TPC-H sample into DuckDB, then transform in-database with dbt.

Measurement notes
-----------------
* `dbt run` is invoked ONCE per sample fraction. Per-model times come from dbt's own
  `target/run_results.json` (time spent executing each model in DuckDB).
  The earlier approach — one `dbt run --select <model>` subprocess per model — mostly
  measured dbt's ~4 s start-up/parsing cost, which is why it barely changed with data size.
* dbt's start-up/parsing overhead is still real, so it is reported separately as
  "dbt CLI overhead".
* Analyses are compiled once (`dbt compile`) and the compiled SQL is executed directly on
  DuckDB, exactly like the ETL side runs its analytical queries.
"""

import json
import os
import re
import subprocess
import sys
from time import perf_counter

import duckdb

from scripts.dbgen import ROOT_DIR, sample_dir
from scripts.dbgen import tables as dbgen_tables
from utils import benchmark_decorator

DBT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYSES_DIR = os.path.join(DBT_PROJECT_DIR, "analyses")
TARGET_DIR = os.path.join(DBT_PROJECT_DIR, "target")
DB_PATH = os.path.join(ROOT_DIR, "data", "elt_database.db")

MART_MODELS = ["dim_region", "dim_date", "dim_customer", "dim_product", "fact_sales"]
SAMPLE_FRACTIONS = [0.1, 0.5, 1.0]


def _dbt(*args, check=True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["dbt", *args, "--profiles-dir", DBT_PROJECT_DIR],
        cwd=DBT_PROJECT_DIR, capture_output=True, text=True, check=check,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Load + transform
# ─────────────────────────────────────────────────────────────────────────────
def load_sources(sample_fraction: float = 1.0) -> None:
    """EL: copy the raw (sampled) Parquet files into DuckDB as-is."""
    print(f"--- [LOAD SOURCES] raw tables into DuckDB (sample={sample_fraction:g}) ---")
    src_dir = sample_dir(sample_fraction)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with duckdb.connect(database=DB_PATH) as con:
        for table in dbgen_tables:
            parquet = os.path.join(src_dir, f"{table}.parquet").replace("\\", "/")
            con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{parquet}')")
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  ✔ {table:<12}: {count:>10,} rows")


def run_models() -> float:
    """T: `dbt run` once. Returns the wall-clock time of the dbt command."""
    print("--- [TRANSFORM] dbt run ---")
    t0 = perf_counter()
    result = _dbt("run", check=False)
    elapsed = perf_counter() - t0
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stdout}\n{result.stderr}")
    return elapsed


def model_timings() -> dict[str, float]:
    """Execution time of each model, from dbt's run_results.json."""
    with open(os.path.join(TARGET_DIR, "run_results.json")) as f:
        results = json.load(f)["results"]
    return {r["unique_id"].split(".")[-1]: r["execution_time"] for r in results}


def row_count(table: str) -> int:
    with duckdb.connect(database=DB_PATH, read_only=True) as con:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def run_compiled_analysis(name: str):
    path = os.path.join(TARGET_DIR, "compiled", "dbt_elt", "analyses", f"{name}.sql")
    with open(path) as f:
        sql = f.read()
    with duckdb.connect(database=DB_PATH, read_only=True) as con:
        return con.execute(sql).fetchdf()


# ─────────────────────────────────────────────────────────────────────────────
# Benchmark (one subprocess per sample fraction so memory is fully released)
# ─────────────────────────────────────────────────────────────────────────────
def _run_single_fraction(fraction: float) -> None:
    run_wall = {}

    def pipeline():
        load_sources(fraction)
        run_wall["dbt run"] = run_models()

    total = benchmark_decorator("Data ingestion and dimensional modeling", pipeline)()
    results = [total.to_dict()]

    timings = model_timings()
    for model in MART_MODELS:
        rows = row_count(model)
        t = timings[model]
        results.append({
            "analysis_name": f"Transform {model}", "execution_time": t, "cpu_time": None,
            "memory_peak": None, "row_count": rows, "throughput": round(rows / t, 2) if t else None,
        })
    overhead = run_wall["dbt run"] - sum(timings.values())
    results.append({
        "analysis_name": "dbt CLI overhead (startup, parsing)", "execution_time": overhead,
        "cpu_time": None, "memory_peak": None, "row_count": None, "throughput": None,
    })
    print(f"\n  dbt run wall time {run_wall['dbt run']:.2f}s = models {sum(timings.values()):.2f}s "
          f"+ CLI overhead {overhead:.2f}s")

    _dbt("compile")  # untimed: produces the compiled analysis SQL
    for file in sorted(os.listdir(ANALYSES_DIR)):
        if not file.endswith(".sql"):
            continue
        name = file[:-4]
        print(f"\n  [Analysis] {name}")
        b = benchmark_decorator(
            analysis_name=name.replace("_", " ").capitalize(),
            func=lambda n=name: run_compiled_analysis(n),
        )()
        print(b.result)
        results.append(b.to_dict())

    for r in results:
        r["sample_fraction"] = fraction
    with open(os.path.join(DBT_PROJECT_DIR, f"_dbt_tmp_f{fraction:g}.json"), "w") as f:
        json.dump(results, f, indent=4)


def launch(fractions=SAMPLE_FRACTIONS, output="dbt_benchmark_results.json") -> list[dict]:
    import pandas as pd

    print("\n" + "=" * 50 + "\nELT pipeline (DuckDB load + dbt)\n" + "=" * 50)
    # Untimed warm-up: the very first dbt invocation does a full project parse (several
    # seconds); later ones reuse the partial-parse cache. Without this, the first sample
    # fraction would be unfairly penalised.
    _dbt("parse")
    all_results = []
    for fraction in fractions:
        print(f"\n{'─' * 50}\n  Sample fraction: {fraction:g}\n{'─' * 50}\n")
        script = (f"import sys; sys.path.insert(0, r'{ROOT_DIR}'); "
                  f"import dbt_project.dbt as dbt; dbt._run_single_fraction({fraction})")
        if subprocess.run([sys.executable, "-c", script], cwd=ROOT_DIR).returncode != 0:
            raise RuntimeError(f"ELT run failed for sample fraction {fraction}")
        tmp = os.path.join(DBT_PROJECT_DIR, f"_dbt_tmp_f{fraction:g}.json")
        with open(tmp) as f:
            all_results.extend(json.load(f))
        os.remove(tmp)

    pd.DataFrame(all_results).to_json(os.path.join(ROOT_DIR, output), orient="records", indent=4)
    print(f"\n{output} written")
    return all_results


# ─────────────────────────────────────────────────────────────────────────────
# Data-quality tests (dbt test)
# ─────────────────────────────────────────────────────────────────────────────
def _execute_dbt_tests() -> str:
    """Run all dbt tests in one command and return the raw output.

    Tests are not run per model: relationship tests reference two models, so any
    per-model selector would either miss or double-count them.
    """
    result = _dbt("test", check=False)
    return result.stdout + result.stderr


def _parse_dbt_test_output(output: str) -> dict:
    """Parse 'Done. PASS=X WARN=Y ERROR=Z SKIP=W [NO-OP=V] TOTAL=N'. WARN + ERROR = failed."""
    pattern = re.compile(
        r"PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)(?:\s+SKIP=\d+)?(?:\s+NO-OP=\d+)?\s+TOTAL=(\d+)",
        re.IGNORECASE,
    )
    m = pattern.search(output)
    if m:
        return {"tests_passed": int(m.group(1)), "tests_failed": int(m.group(2)) + int(m.group(3))}
    return {"tests_passed": 0, "tests_failed": 0}


def run_dbt_tests() -> dict:
    return _parse_dbt_test_output(_execute_dbt_tests())


def launch_tests() -> list[dict]:
    print("\n" + "=" * 50 + "\nDATA-QUALITY TESTS — ELT pipeline (dbt test)\n" + "=" * 50 + "\n")
    b = benchmark_decorator(analysis_name="Tests (all models)", func=_execute_dbt_tests)()
    counts = _parse_dbt_test_output(b.result or "")  # parsed outside the timed section
    bdict = {**b.to_dict(), **counts}
    total = counts["tests_passed"] + counts["tests_failed"]
    print(f"\nRESULT: {counts['tests_passed']}/{total} tests passed")
    return [bdict]
