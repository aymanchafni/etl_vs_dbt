from scripts.dbgen import ROOT_DIR, create_persistent_database
import subprocess
import os
import pandas as pd
from utils import benchmark_decorator

DBT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ANALYSES_DIR = DBT_PROJECT_DIR + "/analyses"

def run_dbt_run():
    print("--- Chargement des tables sources brutes dans DuckDB ---")
    create_persistent_database()

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

def run_dbt_tests(model_name=None) -> dict:
    """
    Lance dbt test (sur un modèle ou sur tous) et retourne
    le nombre de tests passés/échoués parsé depuis stdout.
    """
    cmd = ["dbt", "test", "--quiet"]
    if model_name:
        cmd += ["--select", model_name]

    label = f"Tests {model_name}" if model_name else "Tests (tous modèles)"
    print(f"--- {label} ---")

    try:
        result = subprocess.run(
            cmd, cwd=DBT_PROJECT_DIR, capture_output=True, text=True, check=True
        )
        output = result.stdout + result.stderr
    except subprocess.CalledProcessError as e:
        output = e.stdout + e.stderr
        if e.stdout: print(e.stdout)
        if e.stderr: print(e.stderr)

    # Parser le résumé dbt : "X of Y passed, Z failed"
    passed = failed = 0
    for line in output.splitlines():
        line_l = line.lower()
        if "passed" in line_l and "of" in line_l:
            parts = line_l.replace(",", "").split()
            try:
                idx = parts.index("of")
                passed = int(parts[idx - 1])
                total  = int(parts[idx + 1])
                failed = total - passed
            except (ValueError, IndexError):
                pass

    return {"tests_passed": passed, "tests_failed": failed}


def run_dbt_model(model_name):
    """Lance dbt run sur un seul modèle et retourne le nombre de lignes produites."""
    cmd = ["dbt", "run", "--select", model_name, "--quiet"]
    try:
        subprocess.run(cmd, cwd=DBT_PROJECT_DIR, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Erreur lors de l'exécution de dbt run --select {model_name} :")
        if e.stdout: print(e.stdout)
        if e.stderr: print(e.stderr)
        return None

    # Récupérer le nombre de lignes via dbt show (LIMIT 1 pour juste le count)
    count_cmd = ["dbt", "show", "--select", model_name, "--quiet",
                 "--inline", f"select count(*) as n from {{{{ ref('{model_name}') }}}}"]
    try:
        result = subprocess.run(count_cmd, cwd=DBT_PROJECT_DIR, capture_output=True, text=True, check=True)
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.isdigit():
                return int(line)
    except subprocess.CalledProcessError:
        pass
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

def launch():
    print("\n" + "="*50 + "\n")
    print("Lancement du pipeline ELT (dbt + DuckDB)...\n")

    # Étape 1 : Exécuter dbt run pour créer les tables de dimension et de fait dans DuckDB
    dbt_run_benchmark = benchmark_decorator(
        analysis_name = "Data ingestion and dimensional modeling", 
        func = run_dbt_run
    )()

    results = [
        dbt_run_benchmark.to_dict(),
    ]

    # Étape 2 : Benchmarking granulaire par modèle de la phase Transform (dbt run --select)
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
            results.append(b.to_dict())

    # Étape 3 : Exécuter les analyses définies dans le dossier "analyses" et mesurer leur temps d'exécution
    analysis_files = [f for f in os.listdir(ANALYSES_DIR) if f.endswith(".sql")]

    for file in analysis_files:
        # On retire l'extension .sql pour le sélecteur dbt
        analysis_name = file.replace(".sql", "")
        benchmark = benchmark_decorator(
            analysis_name = analysis_name.replace("_", " ").capitalize(),
            func = lambda name=analysis_name: run_dbt_analysis(name)
        )()

        if benchmark.execution_time:
            results.append(benchmark.to_dict())

    df = pd.DataFrame(results)
    df.to_json('dbt_benchmark_results.json', orient='records', indent=4)
    print("\nLe fichier dbt_benchmark_results.json a été créé avec succès.")


def launch_tests() -> list[dict]:
    """
    Exécute dbt test par modèle, benchmark chaque suite,
    et retourne la liste des benchmarks pour comparaison.
    """
    print("\n" + "=" * 50)
    print("TESTS DE QUALITÉ — Pipeline ELT (dbt test)")
    print("=" * 50 + "\n")

    mart_models    = ["dim_region", "dim_date", "dim_customer", "dim_product", "fact_sales"]
    test_results   = []

    for model in mart_models:
        print(f"\n  [Tests {model}]")
        b = benchmark_decorator(
            analysis_name  = f"Tests {model}",
            func           = lambda m=model: run_dbt_tests(m),
            row_count_func = lambda r: r.get("tests_passed", 0) + r.get("tests_failed", 0),
        )()

        bdict = b.to_dict()
        bdict["tests_passed"] = b.result.get("tests_passed", 0) if b.result else 0
        bdict["tests_failed"] = b.result.get("tests_failed", 0) if b.result else 0

        status = "✅" if bdict["tests_failed"] == 0 else "❌"
        print(f"    {status} {bdict['tests_passed']} passés"
              + (f", {bdict['tests_failed']} échoués" if bdict["tests_failed"] else ""))

        test_results.append(bdict)

    total_passed = sum(r["tests_passed"] for r in test_results)
    total_failed = sum(r["tests_failed"] for r in test_results)
    total        = total_passed + total_failed

    print(f"\n{'=' * 50}")
    print(f"RÉSULTAT : {total_passed}/{total} tests passés"
          + (f"  ⚠️  {total_failed} échecs" if total_failed else "  ✅ Tous passés"))
    print("=" * 50)

    return test_results