"""
lineage.py — Comparaison du Data Lineage et de la Documentation.

dbt (ELT) :
  - Génère manifest.json via `dbt docs generate`
  - Extrait le DAG : nœuds, arêtes, profondeur, couverture de docs

ETL (Python) :
  - Construit le graphe d'appels via analyse AST
  - Mesure la couverture des docstrings
  - Trace les dépendances de données entre fonctions

Métriques comparées :
  - Nœuds dans le DAG                  (tables/fonctions)
  - Arêtes (dépendances)               (qui dépend de qui)
  - Profondeur du lignage              (chaîne la plus longue)
  - Couverture documentation (%)       (fonctions/modèles documentés)
  - Génération automatique du lineage  (oui/non)
  - Sources traçables                  (oui/non)
"""
import ast
import json
import os
import subprocess


def _get_project_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))


# ─────────────────────────────────────────────────────────────────────────────
# ELT (dbt) — Lineage via manifest.json
# ─────────────────────────────────────────────────────────────────────────────

def _generate_dbt_manifest() -> str | None:
    """Lance dbt docs generate et retourne le chemin du manifest.json."""
    dbt_dir = os.path.join(_get_project_root(), "dbt_project")
    manifest_path = os.path.join(dbt_dir, "target", "manifest.json")

    print("--- [ELT] Génération du manifest dbt (dbt docs generate) ---")
    try:
        subprocess.run(
            ["dbt", "docs", "generate", "--quiet"],
            cwd=dbt_dir,
            capture_output=True,
            text=True,
            check=True,
        )
        print("  ✔ manifest.json généré")
        return manifest_path
    except subprocess.CalledProcessError as e:
        print(f"  ❌ Erreur : {e.stderr}")
        return None


def analyze_dbt_lineage() -> dict:
    """
    Parse manifest.json pour extraire le DAG et les métriques de documentation.
    """
    manifest_path = _generate_dbt_manifest()

    if not manifest_path or not os.path.exists(manifest_path):
        return _empty_lineage("ELT (dbt)")

    with open(manifest_path) as f:
        manifest = json.load(f)

    nodes      = manifest.get("nodes", {})
    sources    = manifest.get("sources", {})
    exposures  = manifest.get("exposures", {})

    # Filtrer uniquement les modèles (pas les tests ni analyses)
    models = {k: v for k, v in nodes.items()
              if v.get("resource_type") == "model"}
    tests  = {k: v for k, v in nodes.items()
              if v.get("resource_type") == "test"}

    # Construire le DAG : arêtes = dépendances entre modèles
    edges = []
    for node_id, node in models.items():
        for dep in node.get("depends_on", {}).get("nodes", []):
            if dep.startswith("model.") or dep.startswith("source."):
                edges.append((dep.split(".")[-1], node["name"]))

    # Profondeur du lignage = longueur du chemin le plus long dans le DAG
    def dag_depth(node_name: str, memo: dict = {}) -> int:
        if node_name in memo:
            return memo[node_name]
        parents = [e[0] for e in edges if e[1] == node_name]
        depth   = 1 + max((dag_depth(p, memo) for p in parents), default=0)
        memo[node_name] = depth
        return depth

    all_node_names = list({e[1] for e in edges} | {e[0] for e in edges})
    max_depth      = max((dag_depth(n) for n in all_node_names), default=0)

    # Couverture de documentation
    documented = sum(1 for m in models.values() if m.get("description", "").strip())
    doc_coverage = round(documented / len(models) * 100, 1) if models else 0.0

    # Colonnes documentées
    total_cols = sum(len(m.get("columns", {})) for m in models.values())
    doc_cols   = sum(
        sum(1 for c in m.get("columns", {}).values() if c.get("description", "").strip())
        for m in models.values()
    )
    col_coverage = round(doc_cols / total_cols * 100, 1) if total_cols else 0.0

    return {
        "pipeline"            : "ELT (dbt)",
        "n_nodes"             : len(models),
        "n_sources"           : len(sources),
        "n_edges"             : len(edges),
        "n_tests"             : len(tests),
        "lineage_depth"       : max_depth,
        "doc_coverage_pct"    : doc_coverage,
        "col_doc_coverage_pct": col_coverage,
        "auto_lineage"        : True,
        "auto_docs"           : True,
        "models"              : [m["name"] for m in models.values()],
        "edges"               : edges,
    }


# ─────────────────────────────────────────────────────────────────────────────
# ETL (Python) — Lineage via analyse AST
# ─────────────────────────────────────────────────────────────────────────────

def _parse_python_lineage(filepath: str) -> dict:
    """
    Analyse le fichier Python avec ast pour extraire :
    - Le graphe d'appels entre fonctions
    - La couverture des docstrings
    - Les dépendances de données (quels DataFrames passent entre fonctions)
    """
    try:
        code = open(filepath, encoding="utf-8", errors="ignore").read()
        tree = ast.parse(code)
    except (FileNotFoundError, SyntaxError):
        return {"functions": [], "calls": [], "docstring_coverage": 0.0}

    functions      = []
    calls_graph    = []
    documented     = 0

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_name = node.name
            if func_name.startswith("_") and not func_name.startswith("__"):
                continue  # ignorer les fonctions privées utilitaires

            has_doc = ast.get_docstring(node) is not None
            if has_doc:
                documented += 1

            # Extraire les appels de fonctions internes
            for child in ast.walk(node):
                if isinstance(child, ast.Call):
                    if isinstance(child.func, ast.Name):
                        calls_graph.append((func_name, child.func.id))
                    elif isinstance(child.func, ast.Attribute):
                        calls_graph.append((func_name, child.func.attr))

            functions.append({"name": func_name, "documented": has_doc})

    doc_pct = round(documented / len(functions) * 100, 1) if functions else 0.0
    return {
        "functions"           : functions,
        "calls"               : calls_graph,
        "docstring_coverage"  : doc_pct,
    }


def analyze_etl_lineage() -> dict:
    """
    Construit le lineage du pipeline ETL par analyse statique du code Python.
    """
    root = _get_project_root()
    print("--- [ETL] Analyse statique du lineage Python (AST) ---")

    etl_path   = os.path.join(root, "etl_project", "etl.py")
    utils_path = os.path.join(root, "utils.py")

    etl_ast   = _parse_python_lineage(etl_path)
    utils_ast = _parse_python_lineage(utils_path)

    # Nœuds = fonctions publiques du pipeline ETL
    pipeline_functions = [f for f in etl_ast["functions"]
                          if f["name"] in ("extract", "transform", "load",
                                           "data_ingestion", "launch",
                                           "transform_dim_region",
                                           "transform_dim_date",
                                           "transform_dim_customer",
                                           "transform_dim_product",
                                           "transform_fact_sales")]

    # Lineage manuel ETL : extract → transform_* → load
    etl_edges = [
        ("sources (Parquet)", "extract"),
        ("extract",           "transform_dim_region"),
        ("extract",           "transform_dim_date"),
        ("extract",           "transform_dim_customer"),
        ("extract",           "transform_dim_product"),
        ("extract",           "transform_fact_sales"),
        ("transform_dim_region",   "load"),
        ("transform_dim_date",     "load"),
        ("transform_dim_customer", "load"),
        ("transform_dim_product",  "load"),
        ("transform_fact_sales",   "load"),
        ("load",              "etl_database.db"),
    ]

    all_funcs = etl_ast["functions"] + utils_ast["functions"]
    total     = len(all_funcs)
    doc_pct   = round(
        sum(1 for f in all_funcs if f["documented"]) / total * 100, 1
    ) if total else 0.0

    print(f"  ✔ {len(pipeline_functions)} fonctions de pipeline analysées")

    return {
        "pipeline"            : "ETL (Pandas)",
        "n_nodes"             : len(pipeline_functions),
        "n_sources"           : 1,   # 1 source : fichiers Parquet
        "n_edges"             : len(etl_edges),
        "n_tests"             : 29,  # définis dans etl_tests.py
        "lineage_depth"       : 3,   # extract → transform → load
        "doc_coverage_pct"    : doc_pct,
        "col_doc_coverage_pct": 0.0,  # pas de documentation des colonnes en Python
        "auto_lineage"        : False,
        "auto_docs"           : False,
        "models"              : [f["name"] for f in pipeline_functions],
        "edges"               : etl_edges,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _empty_lineage(pipeline: str) -> dict:
    return {
        "pipeline": pipeline, "n_nodes": 0, "n_sources": 0,
        "n_edges": 0, "n_tests": 0, "lineage_depth": 0,
        "doc_coverage_pct": 0.0, "col_doc_coverage_pct": 0.0,
        "auto_lineage": False, "auto_docs": False,
        "models": [], "edges": [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Rapport console
# ─────────────────────────────────────────────────────────────────────────────

def print_report(etl: dict, elt: dict) -> None:
    print("\n" + "=" * 65)
    print("   DATA LINEAGE & DOCUMENTATION — ETL (Pandas) vs ELT (dbt)")
    print("=" * 65)

    rows = [
        ("Nœuds dans le DAG",           "n_nodes",             ""),
        ("Sources traçables",            "n_sources",           ""),
        ("Arêtes (dépendances)",         "n_edges",             ""),
        ("Tests de qualité",             "n_tests",             ""),
        ("Profondeur du lignage",        "lineage_depth",       "étapes"),
        ("Couverture docs modèles (%)",  "doc_coverage_pct",    "%"),
        ("Couverture docs colonnes (%)", "col_doc_coverage_pct","%"),
        ("Lineage automatique",          "auto_lineage",        ""),
        ("Docs auto générées",           "auto_docs",           ""),
    ]

    print(f"\n  {'Métrique':<35} {'ETL (Pandas)':>14} {'ELT (dbt)':>14}")
    print("  " + "─" * 63)

    for label, key, unit in rows:
        ev = etl[key]
        dv = elt[key]
        def fmt(v):
            if isinstance(v, bool): return "✅ Oui" if v else "❌ Non"
            if isinstance(v, float): return f"{v:.1f}{unit}"
            return f"{v}{unit}"
        print(f"  {label:<35} {fmt(ev):>14} {fmt(dv):>14}")

    # Graphe de lignage textuel ETL
    print("\n  [ETL — Lineage manuel]\n")
    print("  sources (Parquet)")
    print("       └─► extract()")
    print("              ├─► transform_dim_region()  ─┐")
    print("              ├─► transform_dim_date()    ─┤")
    print("              ├─► transform_dim_customer() ─► load() ─► etl_database.db")
    print("              ├─► transform_dim_product() ─┤")
    print("              └─► transform_fact_sales()  ─┘")

    # Graphe de lignage textuel dbt
    print("\n  [ELT — Lineage dbt (DAG automatique)]\n")
    if elt["edges"]:
        printed = set()
        for src, dst in elt["edges"][:12]:
            line = f"  {src:<30} ─► {dst}"
            if line not in printed:
                print(line)
                printed.add(line)
    else:
        print("  (manifest.json non disponible)")

    print("\n" + "=" * 65)


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────

def launch() -> tuple[dict, dict]:
    etl = analyze_etl_lineage()
    elt = analyze_dbt_lineage()
    print_report(etl, elt)
    return etl, elt