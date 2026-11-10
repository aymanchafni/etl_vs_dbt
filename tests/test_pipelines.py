"""End-to-end tests on a tiny TPC-H dataset (SF 0.01, generated once per session).

They guard the properties the benchmark relies on:
  * both pipelines read the same sample, with referential integrity preserved;
  * both pipelines produce the same star schema;
  * memory measurement includes subprocesses (dbt runs DuckDB in a child process).
"""

import os
import shutil
import subprocess
import sys

import duckdb
import pytest

from scripts import dbgen

FRACTION = 0.5


@pytest.fixture(scope="session")
def tiny_tpch():
    if dbgen.count_parquet_tables() < len(dbgen.tables):
        dbgen.generate_data(scale_factor=0.01, overwrite=True)
    dbgen.prepare_samples([FRACTION], overwrite=True)
    return dbgen.sample_dir(FRACTION)


def test_sample_keeps_referential_integrity(tiny_tpch):
    p = lambda t: os.path.join(tiny_tpch, f"{t}.parquet").replace("\\", "/")  # noqa: E731
    full = lambda t: os.path.join(dbgen.DATA_DIR, f"{t}.parquet").replace("\\", "/")  # noqa: E731
    con = duckdb.connect()
    orphans = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{p('lineitem')}') l "
        f"ANTI JOIN read_parquet('{p('orders')}') o ON l.l_orderkey = o.o_orderkey"
    ).fetchone()[0]
    sampled = con.execute(f"SELECT COUNT(*) FROM read_parquet('{p('orders')}')").fetchone()[0]
    total = con.execute(f"SELECT COUNT(*) FROM read_parquet('{full('orders')}')").fetchone()[0]
    assert orphans == 0
    assert sampled / total == pytest.approx(FRACTION, abs=0.03)


@pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt not installed")
def test_etl_and_elt_produce_identical_star_schema(tiny_tpch):
    import verify
    from dbt_project import dbt
    from etl_project import etl

    etl.data_ingestion([], FRACTION)
    dbt.load_sources(FRACTION)
    dbt.run_models()

    results = verify.compare()
    assert all(r["identical"] for r in results), results
    assert {r["table"] for r in results} == set(verify.FINGERPRINTS)


def test_memory_peak_includes_child_processes():
    from utils import benchmark_decorator

    child = "import time; x = bytearray(300 * 1024 * 1024); time.sleep(1)"
    b = benchmark_decorator("child", lambda: subprocess.run([sys.executable, "-c", child], check=True))()
    assert b.memory_peak > 250  # MB: the 300 MB allocated by the subprocess is visible
