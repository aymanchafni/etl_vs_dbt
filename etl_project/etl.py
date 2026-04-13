import gc
import os

import duckdb
import pandas as pd

from scripts.dbgen import ROOT_DIR
from utils import benchmark_decorator

DATA_DIR = os.path.join(ROOT_DIR, "data")
DB_PATH  = os.path.join(DATA_DIR, "etl_database.db")

# ─────────────────────────────────────────────
# QUERIES D'ANALYSE (exécutées sur DuckDB après le Load)
# ─────────────────────────────────────────────
queries = [
    (
        "Total revenue per region and year",
        """
            SELECT
                r.region_name,
                d.year,
                COUNT(DISTINCT f.order_key) AS total_orders,
                ROUND(SUM(f.revenue), 2)    AS total_revenue
            FROM fact_sales f
            JOIN dim_region   r ON f.region_key   = r.region_key
            JOIN dim_date     d ON f.date_key      = d.date_key
            GROUP BY r.region_name, d.year
            ORDER BY d.year DESC, total_revenue DESC;
        """
    ),
    (
        "Total revenue per quarter and year",
        """
            SELECT
                d.year,
                d.quarter,
                COUNT(DISTINCT f.order_key) AS total_orders,
                ROUND(SUM(f.revenue), 2)    AS total_revenue
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.year, d.quarter
            ORDER BY d.year DESC, d.quarter DESC;
        """
    ),
    (
        "Top 5 of market segments by total revenue",
        """
            SELECT
                c.market_segment,
                ROUND(SUM(f.revenue), 2) AS total_revenue,
                SUM(f.quantity)          AS total_qty
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.market_segment
            ORDER BY total_revenue DESC
            LIMIT 5;
        """
    ),
    (
        "Top 5 of products by total revenue",
        """
            SELECT
                p.name AS product_name,
                SUM(f.quantity)          AS total_qty,
                ROUND(SUM(f.revenue), 2) AS total_revenue
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.name
            ORDER BY total_revenue DESC
            LIMIT 5;
        """
    ),
    (
        "Top 5 of customers by total revenue",
        """
            SELECT
                c.name AS customer_name,
                COUNT(DISTINCT f.order_key) AS total_orders,
                ROUND(SUM(f.revenue), 2)    AS total_revenue
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.name
            ORDER BY total_revenue DESC
            LIMIT 5;
        """
    ),
]


# ─────────────────────────────────────────────
# ÉTAPE 1 — EXTRACT
# Lecture des fichiers Parquet bruts → DataFrames Pandas, sans transformation
# ─────────────────────────────────────────────
def extract() -> dict[str, pd.DataFrame]:
    print("--- [EXTRACT] Lecture des fichiers Parquet bruts ---")

    tables = ["lineitem", "orders", "customer", "part", "region", "nation"]
    raw: dict[str, pd.DataFrame] = {}

    for table in tables:
        path = os.path.join(DATA_DIR, f"{table}.parquet")
        raw[table] = pd.read_parquet(path)
        print(f"  ✔ {table:<12}: {len(raw[table]):>10,} lignes chargées")

    print()
    return raw


# ─────────────────────────────────────────────
# ÉTAPE 2 — TRANSFORM
# Chaque table est transformée dans sa propre fonction pour permettre
# un benchmarking granulaire (temps, CPU, débit) par table
# ─────────────────────────────────────────────

def transform_dim_region(region: pd.DataFrame) -> pd.DataFrame:
    return (
        region[["r_regionkey", "r_name"]]
        .drop_duplicates()
        .rename(columns={"r_regionkey": "region_key", "r_name": "region_name"})
        .reset_index(drop=True)
    )

def transform_dim_date(lineitem_f: pd.DataFrame) -> pd.DataFrame:
    return (
        lineitem_f[["l_shipdate"]]
        .drop_duplicates()
        .assign(
            year    = lambda df: df["l_shipdate"].dt.year,
            month   = lambda df: df["l_shipdate"].dt.month,
            day     = lambda df: df["l_shipdate"].dt.day,
            quarter = lambda df: df["l_shipdate"].dt.quarter,
            week    = lambda df: df["l_shipdate"].dt.isocalendar().week.astype(int),
        )
        .rename(columns={"l_shipdate": "date_key"})
        .reset_index(drop=True)
    )

def transform_dim_customer(customer: pd.DataFrame) -> pd.DataFrame:
    return (
        customer[["c_custkey", "c_name", "c_address", "c_phone", "c_acctbal", "c_mktsegment"]]
        .drop_duplicates(subset=["c_custkey"])
        .rename(columns={
            "c_custkey"    : "customer_key",
            "c_name"       : "name",
            "c_address"    : "address",
            "c_phone"      : "phone",
            "c_acctbal"    : "account_balance",
            "c_mktsegment" : "market_segment",
        })
        .reset_index(drop=True)
    )

def transform_dim_product(part: pd.DataFrame) -> pd.DataFrame:
    return (
        part[["p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container"]]
        .drop_duplicates(subset=["p_partkey"])
        .rename(columns={
            "p_partkey"   : "product_key",
            "p_name"      : "name",
            "p_mfgr"      : "manufacturer",
            "p_brand"     : "brand",
            "p_type"      : "type",
            "p_size"      : "size",
            "p_container" : "container",
        })
        .reset_index(drop=True)
    )

def transform_fact_sales(lineitem_f: pd.DataFrame, orders: pd.DataFrame,
                          customer: pd.DataFrame, nation: pd.DataFrame) -> pd.DataFrame:
    customer_with_region = (
        customer[["c_custkey", "c_nationkey"]]
        .merge(nation[["n_nationkey", "n_regionkey"]], left_on="c_nationkey", right_on="n_nationkey", how="left")
        [["c_custkey", "n_regionkey"]]
        .rename(columns={"n_regionkey": "region_key"})
    )
    return (
        lineitem_f[["l_orderkey", "l_partkey", "l_shipdate", "l_quantity", "l_extendedprice", "l_discount"]]
        .merge(orders[["o_orderkey", "o_custkey"]], left_on="l_orderkey", right_on="o_orderkey", how="left")
        .merge(customer_with_region,               left_on="o_custkey",  right_on="c_custkey",  how="left")
        .assign(revenue=lambda df: (df["l_extendedprice"] * (1 - df["l_discount"])).round(2).astype("float64"))
        [["l_orderkey", "l_partkey", "l_shipdate", "o_custkey", "region_key", "l_quantity", "revenue"]]
        .rename(columns={
            "l_orderkey"  : "order_key",
            "l_partkey"   : "product_key",
            "l_shipdate"  : "date_key",
            "o_custkey"   : "customer_key",
            "l_quantity"  : "quantity",
        })
        .reset_index(drop=True)
    )

def transform(raw: dict[str, pd.DataFrame], transform_results: list) -> dict[str, pd.DataFrame]:
    """
    Orchestre les transformations par table et collecte les benchmarks
    granulaires dans transform_results (liste passée par référence).
    """
    print("--- [TRANSFORM] Application des transformations avec Pandas ---")

    lineitem = raw["lineitem"]
    orders   = raw["orders"]
    customer = raw["customer"]
    part     = raw["part"]
    region   = raw["region"]
    nation   = raw["nation"]

    # Pré-filtrage et parsing des dates — commun à dim_date et fact_sales
    lineitem_f = lineitem[lineitem["l_linestatus"] == "F"].copy()
    lineitem_f["l_shipdate"] = pd.to_datetime(lineitem_f["l_shipdate"])

    # ── Benchmarks par table ─────────────────────────────────────────────────
    per_table = [
        ("Transform dim_region",   lambda: transform_dim_region(region)),
        ("Transform dim_date",     lambda: transform_dim_date(lineitem_f)),
        ("Transform dim_customer", lambda: transform_dim_customer(customer)),
        ("Transform dim_product",  lambda: transform_dim_product(part)),
        ("Transform fact_sales",   lambda: transform_fact_sales(lineitem_f, orders, customer, nation)),
    ]

    transformed = {}
    table_keys  = ["dim_region", "dim_date", "dim_customer", "dim_product", "fact_sales"]

    for (label, func), key in zip(per_table, table_keys):
        print(f"\n  [{label}]")
        b = benchmark_decorator(label, func, row_count_func=lambda r: len(r))()
        transformed[key] = b.result
        transform_results.append(b.to_dict())

    print()
    return transformed


# ─────────────────────────────────────────────
# ÉTAPE 3 — LOAD
# Écriture table par table dans un fichier DuckDB sur disque.
# Chaque DataFrame est supprimé immédiatement après l'écriture pour
# ne pas gonfler artificiellement le pic mémoire du processus Python.
# ─────────────────────────────────────────────
def load(transformed: dict[str, pd.DataFrame]) -> None:
    print(f"--- [LOAD] Écriture des tables dans '{DB_PATH}' ---")

    for table_name in list(transformed.keys()):
        df = transformed.pop(table_name)  # retire du dict → libère la référence Python

        with duckdb.connect(database=DB_PATH) as con:
            con.register("_staging", df)
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _staging")
        # Le context manager ferme la connexion : DuckDB flush sur disque immédiatement
        del df
        gc.collect()
        print(f"  ✔ '{table_name}' écrit sur disque et libéré de la mémoire")

    print()


# ─────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────
def data_ingestion(transform_results: list) -> None:
    """
    Orchestre Extract → Transform → Load.
    transform_results : liste collectant les benchmarks par table (passée par référence)
    """
    raw         = extract()
    transformed = transform(raw, transform_results)
    del raw
    gc.collect()
    load(transformed)


def launch():
    print("\n" + "=" * 50 + "\n")
    print("Lancement du pipeline ETL (Python + DuckDB)...\n")
    print("Architecture : Extract (Parquet → Pandas) → Transform (Pandas) → Load (fichier DuckDB)\n")

    transform_results = []  # collecte les benchmarks granulaires par table

    benchmark_data_ingestion = benchmark_decorator(
        analysis_name="Data ingestion and dimensional modeling",
        func=lambda: data_ingestion(transform_results),
    )()

    # Résultats : ingestion globale + détail par table de la phase Transform
    results = [benchmark_data_ingestion.to_dict()] + transform_results

    # -- Affichage des tables chargees ------------------------------------------
    print("\n" + "=" * 50)
    print("TABLES CHARGEES DANS DUCKDB")
    print("=" * 50)

    tables_to_display = [
        ("dim_region",   "Dimension Regions"),
        ("dim_date",     "Dimension Dates"),
        ("dim_customer", "Dimension Clients"),
        ("dim_product",  "Dimension Produits"),
        ("fact_sales",   "Table de Fait - Ventes"),
    ]

    with duckdb.connect(database=DB_PATH, read_only=True) as con:
        for table_name, label in tables_to_display:
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            preview   = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
            print(f"\n{label}  ({row_count:,} lignes au total - apercu des 5 premieres)")
            print(preview.to_string(index=False))

    print("\n" + "=" * 50 + "\n")

    # Les queries lisent depuis le fichier DuckDB sur disque — rien n'est retenu
    # en mémoire Python entre deux requêtes, grâce au context manager
    for i, (description, query) in enumerate(queries):
        print(f"Requête {i + 1} : {description}")

        def run_query(q=query):
            with duckdb.connect(database=DB_PATH, read_only=True) as con:
                return con.execute(q).fetchdf()

        benchmark = benchmark_decorator(
            analysis_name=description,
            func=run_query,
        )()

        print(benchmark.result)

        if benchmark.execution_time:
            results.append(benchmark.to_dict())

    df = pd.DataFrame(results)
    df.to_json("etl_benchmark_results.json", orient="records", indent=4)
    print("\nLe fichier etl_benchmark_results.json a été créé avec succès.")