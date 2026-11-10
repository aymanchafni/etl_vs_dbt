"""TPC-H data generation (DuckDB `tpch` extension) and shared, consistent sampling.

Both pipelines read exactly the same sampled Parquet files, so they process identical
input. Sampling is done on *order keys* and line items follow their order, which keeps
referential integrity (every sampled line item still has its order).
"""

import os
import shutil

import duckdb

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
SAMPLES_DIR = os.path.join(DATA_DIR, "samples")

# Tables of the TPC-H dataset
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
# Fact tables are sampled; dimension/reference tables are always copied in full.
SAMPLED_TABLES = {"orders", "lineitem"}


def is_table_exported(table):
    return os.path.exists(os.path.join(DATA_DIR, f"{table}.parquet"))


def count_parquet_tables():
    """Number of TPC-H tables already exported as Parquet."""
    return sum(is_table_exported(t) for t in tables)


def generate_data(scale_factor, overwrite=False):
    """Generate TPC-H at the given scale factor (SF 1 ~ 1 GB) and export it to Parquet."""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = duckdb.connect(database=":memory:")
    conn.execute("INSTALL tpch;")
    conn.execute("LOAD tpch;")
    conn.execute(f"CALL dbgen(sf={float(scale_factor)});")

    for table in tables:
        output_path = os.path.join(DATA_DIR, f"{table}.parquet")
        if is_table_exported(table):
            if not overwrite:
                print(f"⚠️ {table} already exists, skipped (use overwrite=True to regenerate).")
                continue
            os.remove(output_path)
        print(f"Exporting {table} to Parquet...")
        conn.execute(f"COPY {table} TO '{output_path}' (FORMAT PARQUET);")

    conn.close()
    # Generated data changed: previously prepared samples are stale.
    shutil.rmtree(SAMPLES_DIR, ignore_errors=True)
    print("✅ TPC-H generation and export done")


def sample_dir(fraction: float) -> str:
    return os.path.join(SAMPLES_DIR, f"f{fraction:g}")


def prepare_samples(fractions, overwrite=False):
    """Write one directory of Parquet files per sample fraction.

    Orders are selected with a deterministic hash of o_orderkey (same sample on every run
    and for both pipelines); line items are kept iff their order is selected.
    """
    con = duckdb.connect(database=":memory:")
    for fraction in fractions:
        out_dir = sample_dir(fraction)
        if os.path.isdir(out_dir) and not overwrite:
            continue
        os.makedirs(out_dir, exist_ok=True)
        threshold = int(round(fraction * 10_000))
        src = lambda t: os.path.join(DATA_DIR, f"{t}.parquet").replace("\\", "/")  # noqa: E731
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE sampled_orders AS SELECT * FROM read_parquet('{src('orders')}') "
            f"WHERE hash(o_orderkey) % 10000 < {threshold}"
        )
        for table in tables:
            dst = os.path.join(out_dir, f"{table}.parquet").replace("\\", "/")
            if table == "orders":
                query = "SELECT * FROM sampled_orders"
            elif table == "lineitem":
                query = (f"SELECT l.* FROM read_parquet('{src('lineitem')}') l "
                         "SEMI JOIN sampled_orders o ON l.l_orderkey = o.o_orderkey")
            else:
                query = f"SELECT * FROM read_parquet('{src(table)}')"
            con.execute(f"COPY ({query}) TO '{dst}' (FORMAT PARQUET)")
        n = con.execute("SELECT COUNT(*) FROM sampled_orders").fetchone()[0]
        print(f"  ✔ sample {fraction:g}: {n:,} orders -> {out_dir}")
    con.close()
