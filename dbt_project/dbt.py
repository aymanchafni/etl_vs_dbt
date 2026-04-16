from scripts.dbgen import ROOT_DIR, create_persistent_database, tables as dbgen_tables
import subprocess
import os
import pandas as pd
from utils import benchmark_decorator

DBT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYSES_DIR = DBT_PROJECT_DIR + "/analyses"

# Tables de référence — toujours chargées complètes
_FULL_TABLES   = {"customer", "part", "region", "nation", "partsupp", "supplier"}
# Tables de fait — échantillonnées selon sample_fraction
_SAMPLE_TABLES = {"lineitem", "orders"}


def _load_sources_with_sample(sample_fraction: float = 1.0) -> None:
    """
    Charge les tables sources dans elt_database.db en appliquant
    un échantillonnage sur lineitem et orders via DuckDB USING SAMPLE.
    """
    import duckdb as _duckdb
    db_path = os.path.join(ROOT_DIR, "data", "elt_database.db")
    print(f"--- [LOAD SOURCES] Chargement des tables sources (sample={sample_fraction}) ---")

    with _duckdb.connect(database=db_path) as con:
        for table in dbgen_tables:
            parquet = os.path.join(ROOT_DIR, "data", f"{table}.parquet")
            if not os.path.exists(parquet):
                continue
            if table in _SAMPLE_TABLES and sample_fraction < 1.0:
                pct   = sample_fraction * 100
                query = (f"CREATE OR REPLACE TABLE {table} AS "
                         f"SELECT * FROM read_parquet('{parquet}') "
                         f"USING SAMPLE {pct}% (bernoulli, 42);")
            else:
                query = (f"CREATE OR REPLACE TABLE {table} AS "
                         f"SELECT * FROM read_parquet('{parquet}');")
            con.execute(query)
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  ✔ {table:<12}: {count:>10,} lignes chargées")
    print()


def run_dbt_run(sample_fraction: float = 1.0):
    print("--- Chargement des tables sources brutes dans DuckDB ---")
    _load_sources_with_sample(sample_fraction)

    print("--- Exécution de dbt run pour créer les tables de dimension et de fait dans DuckDB ---")

    cmd = ["dbt", "run", "--quiet"]

    try:
        subprocess.run(
            cmd,
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de dbt run : {e.stderr}")

def run_dbt_compile():
    print("--- Compilation des modèles dbt pour vérifier la syntaxe et la configuration ---")

    cmd = ["dbt", "compile", "--quiet"]
    
    try:
        subprocess.run(
            cmd, 
            cwd=DBT_PROJECT_DIR, 
            capture_output=True, 
            text=True, 
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de dbt compile : {e.stderr}")

def _execute_dbt_tests() -> str:
    """
    Exécute tous les tests dbt en une seule commande et retourne le stdout brut.

    On ne lance pas par modèle car les relationship tests référencent deux modèles
    (fact_sales + une dim), ce qui rend tout sélecteur per-modèle soit incomplet
    (tests manquants) soit redondant (double-comptage).
    Un seul `dbt test` global garantit exactement 29 tests — identique à l'ETL.

    IMPORTANT : pas de --quiet — supprime la ligne "Done. PASS=X..." dont le regex a besoin.
    """
    cmd = ["dbt", "test"]
    try:
        result = subprocess.run(
            cmd, cwd=DBT_PROJECT_DIR, capture_output=True, text=True, check=True
        )
        return result.stdout + result.stderr
    except subprocess.CalledProcessError as e:
        return (e.stdout or "") + (e.stderr or "")


def _parse_dbt_test_output(output: str) -> dict:
    """
    Parse le résumé dbt hors chrono via regex sur la ligne :
    'Done. PASS=X WARN=Y ERROR=Z SKIP=W NO-OP=W TOTAL=N'
    NO-OP= est optionnel (introduit en dbt 1.8+).
    WARN + ERROR comptent comme des tests échoués.
    """
    import re
    pattern = re.compile(
        r'PASS=(\d+)\s+WARN=(\d+)\s+ERROR=(\d+)'
        r'(?:\s+SKIP=\d+)?(?:\s+NO-OP=\d+)?\s+TOTAL=(\d+)',
        re.IGNORECASE
    )
    m = pattern.search(output)
    if m:
        passed = int(m.group(1))
        warn   = int(m.group(2))
        error  = int(m.group(3))
        return {"tests_passed": passed, "tests_failed": warn + error}
    return {"tests_passed": 0, "tests_failed": 0}

def run_dbt_tests(model_name=None) -> dict:
    """Wrapper public : exécution + parsing (pour usage direct hors benchmark)."""
    return _parse_dbt_test_output(_execute_dbt_tests(model_name))


def run_dbt_model(model_name):
    """
    Lance dbt run sur un seul modèle et retourne le nombre de lignes produites.
    Le row count est lu directement depuis elt_database.db — plus fiable que dbt show --inline.
    """
    import duckdb as _duckdb
    from scripts.dbgen import ROOT_DIR as _ROOT

    cmd = ["dbt", "run", "--select", model_name, "--quiet"]
    try:
        subprocess.run(cmd, cwd=DBT_PROJECT_DIR, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de dbt run --select {model_name} :")
        if e.stdout: print(e.stdout)
        if e.stderr: print(e.stderr)
        return None

    # Lire le row count directement depuis le fichier DuckDB — fiable et sans subprocess supplémentaire
    db_path = os.path.join(_ROOT, "data", "elt_database.db")
    try:
        with _duckdb.connect(database=db_path, read_only=True) as con:
            return con.execute(f"SELECT COUNT(*) FROM {model_name}").fetchone()[0]
    except Exception:
        return None

def run_dbt_analysis(analysis_name):
    print(f"--- Exécution de l'analyse : \"{analysis_name.replace('_', ' ')}\" ---")
    
    # --quiet évite les logs inutiles
    cmd = ["dbt", "show", "--select", analysis_name, "--quiet"]
        
    try:
        result = subprocess.run(
            cmd, 
            cwd=DBT_PROJECT_DIR, 
            capture_output=True, 
            text=True, 
            check=True
        )
    
        print(f"\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de {analysis_name} : {e.stderr}")
        return None

SCALE_FACTORS = [0.1, 0.5, 1.0]


def _run_single_scale(sf: float) -> None:
    """
    Exécute le pipeline ELT pour un seul scale factor et sauvegarde
    les résultats dans un fichier JSON temporaire.
    Appelé dans un subprocess isolé pour libérer toute la mémoire après.
    """
    import json

    dbt_run_benchmark = benchmark_decorator(
        analysis_name="Data ingestion and dimensional modeling",
        func=lambda: run_dbt_run(sf)
    )()

    sf_results = [dbt_run_benchmark.to_dict()]

    # Phase Transform — benchmarking par modèle
    print("\n--- Benchmarking par modèle dbt (phase Transform) ---")
    mart_models = ["dim_region", "dim_date", "dim_customer", "dim_product", "fact_sales"]
    for model in mart_models:
        print(f"\n  [Transform {model}]")
        b = benchmark_decorator(
            analysis_name  = f"Transform {model}",
            func           = lambda m=model: run_dbt_model(m),
            row_count_func = lambda r: r if isinstance(r, int) else None,
        )()
        if b.execution_time:
            sf_results.append(b.to_dict())

    # Phase Analyse
    analysis_files = [f for f in os.listdir(ANALYSES_DIR) if f.endswith(".sql")]
    for file in analysis_files:
        analysis_name = file.replace(".sql", "")
        benchmark = benchmark_decorator(
            analysis_name=analysis_name.replace("_", " ").capitalize(),
            func=lambda name=analysis_name: run_dbt_analysis(name)
        )()
        if benchmark.execution_time:
            sf_results.append(benchmark.to_dict())

    for r in sf_results:
        r["scale_factor"] = sf

    tmp_path = os.path.join(DBT_PROJECT_DIR, f"_dbt_tmp_sf{sf}.json")
    with open(tmp_path, "w") as f:
        json.dump(sf_results, f, indent=4)


def launch():
    import subprocess, sys, json

    print("\n" + "="*50 + "\n")
    print("Lancement du pipeline ELT (dbt + DuckDB)...\n")

    all_results = []

    for sf in SCALE_FACTORS:
        print(f"\n{'─'*50}")
        print(f"  Scale factor : {sf} ({int(sf*100)}% des données)")
        print(f"{'─'*50}\n")

        # Chaque scale factor tourne dans un subprocess isolé
        # ROOT_DIR pointe sur la racine du projet (parent de dbt_project/)
        project_root = os.path.dirname(DBT_PROJECT_DIR)
        script = (
            f"import sys; sys.path.insert(0, r'{os.path.dirname(DBT_PROJECT_DIR)}'); "
            f"import dbt_project.dbt as dbt; dbt._run_single_scale({sf})"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=DBT_PROJECT_DIR,
        )

        if result.returncode != 0:
            print(f"  ❌ Erreur lors du run sf={sf}")
            continue

        tmp_path = os.path.join(DBT_PROJECT_DIR, f"_dbt_tmp_sf{sf}.json")
        if os.path.exists(tmp_path):
            with open(tmp_path) as f:
                all_results.extend(json.load(f))
            os.remove(tmp_path)

    df = pd.DataFrame(all_results)
    df.to_json('dbt_benchmark_results.json', orient='records', indent=4)
    print("\nLe fichier dbt_benchmark_results.json a été créé avec succès.")




def launch_tests() -> list[dict]:
    """
    Exécute tous les tests dbt en une seule commande benchmarkée.
    Un seul run global = 29 tests, identique au périmètre ETL.
    """
    print("\n" + "=" * 50)
    print("TESTS DE QUALITÉ — Pipeline ELT (dbt test)")
    print("=" * 50 + "\n")

    # Benchmark de l'exécution SQL uniquement — parsing hors chrono
    b = benchmark_decorator(
        analysis_name = "Tests (tous modèles)",
        func          = _execute_dbt_tests,
    )()

    # Parsing hors chrono — pas de biais sur la mesure
    counts = _parse_dbt_test_output(b.result or "")

    bdict = b.to_dict()
    bdict["tests_passed"] = counts["tests_passed"]
    bdict["tests_failed"] = counts["tests_failed"]

    status = "✅" if bdict["tests_failed"] == 0 else "❌"
    print(f"\n  {status} {bdict['tests_passed']} passés"
          + (f", {bdict['tests_failed']} échoués" if bdict["tests_failed"] else ""))

    total = bdict["tests_passed"] + bdict["tests_failed"]
    print(f"\n{'=' * 50}")
    print(f"RÉSULTAT : {bdict['tests_passed']}/{total} tests passés"
          + (f"  ⚠️  {bdict['tests_failed']} échecs" if bdict["tests_failed"] else "  ✅ Tous passés"))
    print("=" * 50)

    return [bdict]