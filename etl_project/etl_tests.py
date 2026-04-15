"""
etl_tests.py — Tests manuels de qualité des données pour le pipeline ETL.

Chaque test est l'équivalent exact d'un test dbt (not_null, unique,
relationships, accepted_values) appliqué sur les tables chargées dans
etl_database.db. Les tests sont benchmarkés individuellement pour
permettre une comparaison directe avec dbt test.
"""
import gc
import duckdb
import pandas as pd
from scripts.dbgen import ROOT_DIR
import os
from utils import benchmark_decorator

DB_PATH = os.path.join(ROOT_DIR, "data", "etl_database.db")

# ─────────────────────────────────────────────────────────────────────────────
# Primitives de test — équivalents des tests dbt built-in
# ─────────────────────────────────────────────────────────────────────────────

def test_not_null(df: pd.DataFrame, column: str) -> dict:
    """Équivalent : dbt test not_null"""
    failures = int(df[column].isna().sum())
    return {
        "test"    : f"not_null({column})",
        "passed"  : failures == 0,
        "failures": failures,
    }

def test_unique(df: pd.DataFrame, column: str) -> dict:
    """Équivalent : dbt test unique"""
    failures = int(df[column].duplicated().sum())
    return {
        "test"    : f"unique({column})",
        "passed"  : failures == 0,
        "failures": failures,
    }

def test_accepted_values(df: pd.DataFrame, column: str, values: list) -> dict:
    """Équivalent : dbt test accepted_values"""
    mask     = ~df[column].isin(values)
    failures = int(mask.sum())
    return {
        "test"    : f"accepted_values({column})",
        "passed"  : failures == 0,
        "failures": failures,
    }

def test_relationships(df: pd.DataFrame, fk_col: str,
                       ref_df: pd.DataFrame, pk_col: str) -> dict:
    """Équivalent : dbt test relationships"""
    failures = int((~df[fk_col].isin(ref_df[pk_col])).sum())
    return {
        "test"    : f"relationships({fk_col} -> {pk_col})",
        "passed"  : failures == 0,
        "failures": failures,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Suites de tests par table
# ─────────────────────────────────────────────────────────────────────────────

def run_tests_dim_region(tables: dict) -> list[dict]:
    df = tables["dim_region"]
    return [
        test_not_null(df, "region_key"),
        test_unique(df, "region_key"),
        test_not_null(df, "region_name"),
    ]

def run_tests_dim_date(tables: dict) -> list[dict]:
    df = tables["dim_date"]
    return [
        test_not_null(df, "date_key"),
        test_unique(df, "date_key"),
        test_not_null(df, "year"),
        test_not_null(df, "month"),
        test_accepted_values(df, "month", list(range(1, 13))),
        test_not_null(df, "quarter"),
        test_accepted_values(df, "quarter", [1, 2, 3, 4]),
    ]

def run_tests_dim_customer(tables: dict) -> list[dict]:
    df = tables["dim_customer"]
    return [
        test_not_null(df, "customer_key"),
        test_unique(df, "customer_key"),
        test_not_null(df, "name"),
        test_not_null(df, "market_segment"),
        test_accepted_values(df, "market_segment",
                             ["AUTOMOBILE", "BUILDING", "FURNITURE",
                              "HOUSEHOLD", "MACHINERY"]),
    ]

def run_tests_dim_product(tables: dict) -> list[dict]:
    df = tables["dim_product"]
    return [
        test_not_null(df, "product_key"),
        test_unique(df, "product_key"),
        test_not_null(df, "name"),
    ]

def run_tests_fact_sales(tables: dict) -> list[dict]:
    df = tables["fact_sales"]
    return [
        test_not_null(df, "order_key"),
        test_not_null(df, "product_key"),
        test_relationships(df, "product_key", tables["dim_product"], "product_key"),
        test_not_null(df, "date_key"),
        test_relationships(df, "date_key",    tables["dim_date"],    "date_key"),
        test_not_null(df, "customer_key"),
        test_relationships(df, "customer_key", tables["dim_customer"], "customer_key"),
        test_not_null(df, "region_key"),
        test_relationships(df, "region_key",  tables["dim_region"],  "region_key"),
        test_not_null(df, "quantity"),
        test_not_null(df, "revenue"),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Runner principal
# ─────────────────────────────────────────────────────────────────────────────

SUITES = [
    ("dim_region",   run_tests_dim_region),
    ("dim_date",     run_tests_dim_date),
    ("dim_customer", run_tests_dim_customer),
    ("dim_product",  run_tests_dim_product),
    ("fact_sales",   run_tests_fact_sales),
]

def load_tables() -> dict:
    """Charge toutes les tables depuis etl_database.db en mémoire Pandas."""
    tables = {}
    with duckdb.connect(database=DB_PATH, read_only=True) as con:
        for name in ["dim_region", "dim_date", "dim_customer",
                     "dim_product", "fact_sales"]:
            tables[name] = con.execute(f"SELECT * FROM {name}").fetchdf()
    return tables

def launch() -> list[dict]:
    """
    Exécute toutes les suites de tests, benchmark chaque suite,
    affiche les résultats et retourne la liste des benchmarks.
    """
    print("\n" + "=" * 50)
    print("TESTS DE QUALITÉ — Pipeline ETL (Pandas)")
    print("=" * 50 + "\n")

    print("Chargement des tables depuis etl_database.db...")
    tables = load_tables()

    benchmark_results = []
    all_tests         = []
    total_passed      = 0
    total_failed      = 0

    for model_name, suite_func in SUITES:
        print(f"\n  [Tests {model_name}]")

        def run_suite(func=suite_func, t=tables):
            return func(t)

        b = benchmark_decorator(
            analysis_name  = f"Tests {model_name}",
            func           = run_suite,
            row_count_func = lambda r: len(r),   # nombre de tests exécutés
        )()

        suite_results = b.result
        passed  = sum(1 for r in suite_results if r["passed"])
        failed  = sum(1 for r in suite_results if not r["passed"])
        total_passed += passed
        total_failed += failed

        for r in suite_results:
            status = "✅" if r["passed"] else "❌"
            print(f"    {status} {r['test']}"
                  + (f"  ({r['failures']} échecs)" if not r["passed"] else ""))

        all_tests.extend(suite_results)

        bdict = b.to_dict()
        bdict["tests_passed"] = passed
        bdict["tests_failed"] = failed
        benchmark_results.append(bdict)

        gc.collect()

    # Résumé global
    print(f"\n{'=' * 50}")
    total = total_passed + total_failed
    print(f"RÉSULTAT : {total_passed}/{total} tests passés"
          + (f"  ⚠️  {total_failed} échecs" if total_failed else "  ✅ Tous passés"))
    print("=" * 50)

    return benchmark_results