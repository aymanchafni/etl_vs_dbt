"""Build static charts (docs/*.png) and a results table (docs/results.md) from the JSON
results written by the other commands."""

import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(ROOT, "docs")
COLORS = {"ETL (pandas)": "#E69F00", "ELT (dbt)": "#0072B2"}
TOTAL = "Data ingestion and dimensional modeling"
MODELS = ["dim_region", "dim_date", "dim_customer", "dim_product", "fact_sales"]


def _load(name):
    path = os.path.join(ROOT, name)
    return json.load(open(path)) if os.path.exists(path) else None


def _results() -> pd.DataFrame:
    etl = pd.DataFrame(_load("etl_benchmark_results.json")).assign(pipeline="ETL (pandas)")
    elt = pd.DataFrame(_load("dbt_benchmark_results.json")).assign(pipeline="ELT (dbt)")
    return pd.concat([etl, elt], ignore_index=True)


def _grouped_bars(ax, df, x_col, y_col, x_labels):
    width = 0.38
    for i, (pipeline, color) in enumerate(COLORS.items()):
        sub = df[df["pipeline"] == pipeline].set_index(x_col)
        ys = [float(sub.loc[x, y_col]) if x in sub.index else 0 for x in x_labels]
        xs = [j + (i - 0.5) * width for j in range(len(x_labels))]
        bars = ax.bar(xs, ys, width, label=pipeline, color=color)
        ax.bar_label(bars, fmt="%.1f", fontsize=8, padding=2)
    ax.set_xticks(range(len(x_labels)))
    ax.spines[["top", "right"]].set_visible(False)
    ax.legend(frameon=False)


def build():
    os.makedirs(DOCS, exist_ok=True)
    df = _results()
    total = df[df["analysis_name"] == TOTAL]
    fractions = sorted(total["sample_fraction"].unique())
    labels = [f"{f:g}" for f in fractions]

    # 1. End-to-end time and peak memory per sample fraction
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    _grouped_bars(axes[0], total, "sample_fraction", "execution_time", fractions)
    axes[0].set(title="End-to-end time (extract + transform + load)", ylabel="seconds",
                xlabel="sample fraction of TPC-H SF1 orders")
    axes[0].set_xticklabels(labels)
    _grouped_bars(axes[1], total, "sample_fraction", "memory_peak", fractions)
    axes[1].set(title="Peak memory (process tree)", ylabel="MB", xlabel="sample fraction")
    axes[1].set_xticklabels(labels)
    fig.tight_layout()
    fig.savefig(os.path.join(DOCS, "end_to_end.png"), dpi=150)
    plt.close(fig)

    # 2. Per-model transform time at the largest fraction
    biggest = max(fractions)
    per_model = df[(df["sample_fraction"] == biggest) & df["analysis_name"].str.startswith("Transform ")]
    per_model = per_model.assign(model=per_model["analysis_name"].str.replace("Transform ", ""))
    fig, ax = plt.subplots(figsize=(9, 4))
    _grouped_bars(ax, per_model, "model", "execution_time", MODELS)
    ax.set_xticklabels(MODELS)
    overhead = df[(df["sample_fraction"] == biggest) & df["analysis_name"].str.startswith("dbt CLI")]
    note = f" — dbt start-up overhead: {overhead['execution_time'].iloc[0]:.1f}s" if len(overhead) else ""
    ax.set(title=f"Transform time per model (sample {biggest:g}){note}", ylabel="seconds")
    fig.tight_layout()
    fig.savefig(os.path.join(DOCS, "per_model.png"), dpi=150)
    plt.close(fig)

    _write_markdown(df, total, fractions)
    print(f"Report written to {DOCS}")


def _write_markdown(df, total, fractions):
    lines = ["# Results", "", "## End-to-end (extract + transform + load)", "",
             "| Sample | Pipeline | Time (s) | CPU (s) | Peak memory (MB) |", "|---|---|---|---|---|"]
    for f in fractions:
        for pipeline in COLORS:
            r = total[(total["sample_fraction"] == f) & (total["pipeline"] == pipeline)]
            if len(r):
                r = r.iloc[0]
                lines.append(f"| {f:g} | {pipeline} | {r.execution_time:.2f} | {r.cpu_time:.2f} "
                             f"| {r.memory_peak:,.0f} |")

    biggest = max(fractions)
    lines += ["", f"## Transform time per model (sample {biggest:g})", "",
              "| Model | Rows | ETL (pandas) s | ELT (dbt) s |", "|---|---|---|---|"]
    sub = df[df["sample_fraction"] == biggest]
    for m in MODELS:
        e = sub[(sub["analysis_name"] == f"Transform {m}") & (sub["pipeline"] == "ETL (pandas)")]
        d = sub[(sub["analysis_name"] == f"Transform {m}") & (sub["pipeline"] == "ELT (dbt)")]
        if len(e) and len(d):
            lines.append(f"| {m} | {int(e.iloc[0].row_count):,} | {e.iloc[0].execution_time:.3f} | "
                         f"{d.iloc[0].execution_time:.3f} |")
    oh = sub[sub["analysis_name"].str.startswith("dbt CLI")]
    if len(oh):
        lines.append(f"\ndbt CLI start-up/parsing overhead: **{oh.iloc[0].execution_time:.2f} s** per `dbt run`.")

    tests_etl, tests_elt = _load("etl_test_results.json"), _load("dbt_test_results.json")
    if tests_etl and tests_elt:
        def summary(rows):
            p = sum(r["tests_passed"] for r in rows)
            f = sum(r["tests_failed"] for r in rows)
            t = sum(r["execution_time"] for r in rows)
            return p, f, t
        (pe, fe, te), (pd_, fd, td) = summary(tests_etl), summary(tests_elt)
        lines += ["", "## Data-quality tests", "", "| Pipeline | Passed | Failed | Time (s) |", "|---|---|---|---|",
                  f"| ETL (hand-written pandas checks) | {pe} | {fe} | {te:.2f} |",
                  f"| ELT (`dbt test`, declared in YAML) | {pd_} | {fd} | {td:.2f} |"]

    cx = _load("complexity_results.json")
    if cx:
        lines += ["", "## Development complexity", "", "| Metric | ETL | ELT |", "|---|---|---|"]
        for key, label in [("n_files", "Files"), ("total_sloc", "Source lines (SLOC)"),
                           ("avg_cc", "Avg. cyclomatic complexity"), ("avg_mi", "Avg. maintainability index")]:
            a, b = cx["etl"].get(key), cx["elt"].get(key)
            if a is not None and b is not None:
                lines.append(f"| {label} | {float(a):.1f} | {float(b):.1f} |")

    with open(os.path.join(DOCS, "results.md"), "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    build()
