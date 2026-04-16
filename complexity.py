"""
complexity.py — Mesure de la complexité de développement des deux pipelines.

Métriques collectées via radon :
  - LOC  : Lines Of Code (total)
  - SLOC : Source Lines Of Code (sans commentaires ni lignes vides)
  - CC   : Complexité Cyclomatique moyenne (nombre de chemins logiques)
  - MI   : Maintainability Index (0–100, plus haut = plus maintenable)
  - Comments : Lignes de commentaires
  - Fichiers  : Nombre de fichiers source

Comparaison ETL (Python impératif) vs ELT (dbt déclaratif SQL + YAML).
"""
import os
from radon.complexity import cc_visit
from radon.raw       import analyze
from radon.metrics   import mi_visit


# ─────────────────────────────────────────────────────────────────────────────
# Fichiers de chaque pipeline
# ─────────────────────────────────────────────────────────────────────────────
def _get_project_root() -> str:
    # complexity.py est à la racine du projet — pas besoin de remonter
    return os.path.dirname(os.path.abspath(__file__))


def _etl_files() -> list[tuple[str, str]]:
    """Retourne les fichiers Python du pipeline ETL avec leur étiquette."""
    root = _get_project_root()
    return [
        (os.path.join(root, "etl_project", "etl.py"),       "etl.py"),
        (os.path.join(root, "etl_project", "etl_tests.py"), "etl_tests.py"),
        (os.path.join(root, "utils.py"),                     "utils.py"),
    ]


def _elt_files() -> list[tuple[str, str]]:
    """Retourne les fichiers SQL + YAML + Python du pipeline ELT (dbt)."""
    root     = _get_project_root()
    dbt_dir  = os.path.join(root, "dbt_project")
    marts    = os.path.join(dbt_dir, "models", "marts")
    staging  = os.path.join(dbt_dir, "models", "staging")
    analyses = os.path.join(dbt_dir, "analyses")

    files = [
        (os.path.join(dbt_dir, "dbt.py"),          "dbt.py"),
        (os.path.join(dbt_dir, "dbt_project.yml"), "dbt_project.yml"),
        (os.path.join(dbt_dir, "profiles.yml"),    "profiles.yml"),
    ]

    for folder, label_prefix in [(marts, "marts"), (staging, "staging"), (analyses, "analyses")]:
        if os.path.exists(folder):
            for f in os.listdir(folder):
                if f.endswith((".sql", ".yml", ".yaml")):
                    files.append((os.path.join(folder, f), f"{label_prefix}/{f}"))

    return files


# ─────────────────────────────────────────────────────────────────────────────
# Analyse d'un fichier
# ─────────────────────────────────────────────────────────────────────────────
def _analyze_file(path: str, label: str) -> dict:
    """Analyse un fichier source et retourne ses métriques."""
    ext = os.path.splitext(path)[1]
    try:
        code = open(path, encoding="utf-8", errors="ignore").read()
    except FileNotFoundError:
        return {"file": label, "type": ext.lstrip(".").upper() or "?",
                "loc": 0, "sloc": 0, "comments": 0,
                "blank": 0, "avg_cc": None, "mi": None}

    raw  = analyze(code)

    # CC et MI uniquement pour les fichiers Python
    avg_cc = None
    mi     = None
    if ext == ".py":
        results = cc_visit(code)
        avg_cc  = round(sum(r.complexity for r in results) / len(results), 2) if results else 0.0
        mi      = round(mi_visit(code, multi=True), 2)

    return {
        "file"    : label,
        "type"    : ext.lstrip(".").upper(),
        "loc"     : raw.loc,
        "sloc"    : raw.sloc,
        "comments": raw.comments,
        "blank"   : raw.blank,
        "avg_cc"  : avg_cc,
        "mi"      : mi,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Analyse complète d'un pipeline
# ─────────────────────────────────────────────────────────────────────────────
def analyze_pipeline(files: list[tuple[str, str]], pipeline_name: str) -> dict:
    """
    Analyse tous les fichiers d'un pipeline et retourne un résumé.
    """
    file_metrics = [_analyze_file(path, label) for path, label in files]

    total_loc      = sum(m["loc"]      for m in file_metrics)
    total_sloc     = sum(m["sloc"]     for m in file_metrics)
    total_comments = sum(m["comments"] for m in file_metrics)
    total_blank    = sum(m["blank"]    for m in file_metrics)
    n_files        = len(files)

    py_metrics  = [m for m in file_metrics if m["avg_cc"] is not None]
    avg_cc_all  = round(sum(m["avg_cc"] for m in py_metrics) / len(py_metrics), 2) if py_metrics else 0.0
    avg_mi_all  = round(sum(m["mi"]     for m in py_metrics) / len(py_metrics), 2) if py_metrics else 0.0

    # Ratio commentaires / SLOC
    comment_ratio = round(total_comments / total_sloc * 100, 1) if total_sloc else 0.0

    return {
        "pipeline"      : pipeline_name,
        "n_files"       : n_files,
        "total_loc"     : total_loc,
        "total_sloc"    : total_sloc,
        "total_comments": total_comments,
        "total_blank"   : total_blank,
        "comment_ratio" : comment_ratio,
        "avg_cc"        : avg_cc_all,
        "avg_mi"        : avg_mi_all,
        "file_metrics"  : file_metrics,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Interprétation du MI
# ─────────────────────────────────────────────────────────────────────────────
def _mi_label(mi: float) -> str:
    if mi >= 20:
        return "✅ Maintenable"
    elif mi >= 10:
        return "⚠️  Modéré"
    return "❌ Difficile à maintenir"


def _cc_label(cc: float) -> str:
    if cc <= 5:
        return "✅ Simple"
    elif cc <= 10:
        return "⚠️  Modéré"
    return "❌ Complexe"


# ─────────────────────────────────────────────────────────────────────────────
# Affichage console
# ─────────────────────────────────────────────────────────────────────────────
def print_report(etl: dict, elt: dict) -> None:
    print("\n" + "=" * 65)
    print("   COMPLEXITÉ DE DÉVELOPPEMENT — ETL (Pandas) vs ELT (dbt)")
    print("=" * 65)

    cols = ["pipeline", "n_files", "total_loc", "total_sloc",
            "comment_ratio", "avg_cc", "avg_mi"]
    labels = {
        "pipeline"      : "Pipeline",
        "n_files"       : "Fichiers",
        "total_loc"     : "LOC",
        "total_sloc"    : "SLOC",
        "comment_ratio" : "% Commentaires",
        "avg_cc"        : "CC moyen",
        "avg_mi"        : "MI moyen",
    }

    # Tableau comparatif
    print(f"\n{'Métrique':<22} {'ETL (Pandas)':>16} {'ELT (dbt)':>16}")
    print("─" * 56)
    for col in cols[1:]:
        etl_val = etl[col]
        elt_val = elt[col]
        fmt_e   = f"{etl_val:.2f}" if isinstance(etl_val, float) else str(etl_val)
        fmt_d   = f"{elt_val:.2f}" if isinstance(elt_val, float) else str(elt_val)
        print(f"  {labels[col]:<20} {fmt_e:>16} {fmt_d:>16}")

    # Interprétation
    print("\n" + "─" * 56)
    print(f"  CC  moyen → ETL : {_cc_label(etl['avg_cc'])}  |  ELT : {_cc_label(elt['avg_cc'])}")
    print(f"  MI  moyen → ETL : {_mi_label(etl['avg_mi'])}  |  ELT : {_mi_label(elt['avg_mi'])}")

    # Détail par fichier
    print("\n" + "─" * 56)
    print("  Détail par fichier :\n")
    for pipeline, data in [("ETL (Pandas)", etl), ("ELT (dbt)", elt)]:
        print(f"  [{pipeline}]")
        print(f"  {'Fichier':<40} {'Type':>5} {'LOC':>6} {'SLOC':>6} {'CC':>6} {'MI':>7}")
        print(f"  {'─'*40} {'─'*5} {'─'*6} {'─'*6} {'─'*6} {'─'*7}")
        for m in data["file_metrics"]:
            cc_str  = f"{m['avg_cc']:.1f}" if m["avg_cc"] is not None else "N/A"
            mi_str  = f"{m['mi']:.1f}"     if m["mi"]     is not None else "N/A"
            fname   = m["file"][:38] + "…" if len(m["file"]) > 38 else m["file"]
            print(f"  {fname:<40} {m['type']:>5} {m['loc']:>6} {m['sloc']:>6} {cc_str:>6} {mi_str:>7}")
        print()

    print("=" * 65)


# ─────────────────────────────────────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────
def launch() -> tuple[dict, dict]:
    """
    Lance l'analyse de complexité des deux pipelines,
    affiche le rapport et retourne (etl_metrics, elt_metrics).
    """
    etl = analyze_pipeline(_etl_files(), "ETL (Pandas)")
    elt = analyze_pipeline(_elt_files(), "ELT (dbt)")
    print_report(etl, elt)
    return etl, elt