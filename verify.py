"""Check that the ETL (pandas) and ELT (dbt) pipelines produced the same star schema.

A benchmark is only meaningful if both sides compute the same thing. For every table we
compare row counts and column fingerprints (sum / count distinct of key columns).
"""

import os
import sys

import duckdb

from scripts.dbgen import ROOT_DIR

ETL_DB = os.path.join(ROOT_DIR, "data", "etl_database.db")
ELT_DB = os.path.join(ROOT_DIR, "data", "elt_database.db")

FINGERPRINTS = {
    "dim_region": "COUNT(*), COUNT(DISTINCT region_key), COUNT(DISTINCT region_name)",
    "dim_date": "COUNT(*), COUNT(DISTINCT date_key), SUM(year), SUM(quarter), SUM(week)",
    "dim_customer": "COUNT(*), COUNT(DISTINCT customer_key), ROUND(SUM(account_balance), 2)",
    "dim_product": "COUNT(*), COUNT(DISTINCT product_key), SUM(size)",
    "fact_sales": ("COUNT(*), COUNT(DISTINCT order_key), SUM(customer_key), SUM(region_key), "
                   "ROUND(SUM(quantity), 2), ROUND(SUM(revenue), 0)"),
}


def fingerprint(db_path: str, table: str) -> tuple:
    with duckdb.connect(db_path, read_only=True) as con:
        return tuple(con.execute(f"SELECT {FINGERPRINTS[table]} FROM {table}").fetchone())


def compare() -> list[dict]:
    rows = []
    for table in FINGERPRINTS:
        etl, elt = fingerprint(ETL_DB, table), fingerprint(ELT_DB, table)
        # Revenue is rounded per row by pandas (half-to-even) and DuckDB (half-away-from-zero),
        # so allow a relative difference of 1e-6 on numeric fingerprints.
        same = all(
            a == b or (a is not None and b is not None and abs(float(a) - float(b)) <= 1e-6 * max(1, abs(float(a))))
            for a, b in zip(etl, elt)
        )
        rows.append({"table": table, "rows_etl": etl[0], "rows_elt": elt[0], "identical": same})
    return rows


def main() -> int:
    rows = compare()
    for r in rows:
        status = "✅" if r["identical"] else "❌"
        print(f"{status} {r['table']:<13} ETL {r['rows_etl']:>10,} rows | ELT {r['rows_elt']:>10,} rows")
    ok = all(r["identical"] for r in rows)
    print("\nBoth pipelines produce the same star schema." if ok else "\nOutputs differ!")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
