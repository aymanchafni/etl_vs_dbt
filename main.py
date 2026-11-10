"""ETL (Python + pandas) vs ELT (dbt) on TPC-H — command-line entry point.

    python main.py all --sf 1        # full study: generate → sample → ETL → ELT → verify → tests → report
    python main.py generate --sf 1   # TPC-H data (SF 1 ≈ 1 GB) as Parquet
    python main.py samples           # shared samples (10 %, 50 %, 100 % of orders)
    python main.py etl | elt         # run one pipeline benchmark
    python main.py verify            # both pipelines produced the same star schema?
    python main.py tests             # data-quality tests on both sides
    python main.py complexity        # LOC, cyclomatic complexity, maintainability index
    python main.py lineage           # lineage & documentation coverage
    python main.py report            # charts (docs/*.png) + docs/results.md
    python main.py                   # interactive menu
"""

import argparse
import json
import sys

import pandas as pd

SAMPLE_FRACTIONS = [0.1, 0.5, 1.0]


def cmd_generate(args):
    from scripts import dbgen

    dbgen.generate_data(scale_factor=args.sf, overwrite=args.overwrite)


def cmd_samples(args):
    from scripts import dbgen

    dbgen.prepare_samples(args.fractions, overwrite=True)


def cmd_etl(args):
    from etl_project import etl

    etl.launch(args.fractions)


def cmd_elt(args):
    from dbt_project import dbt

    dbt.launch(args.fractions)


def cmd_verify(args):
    import verify

    if verify.main() != 0:
        sys.exit(1)


def cmd_tests(args):
    from dbt_project import dbt
    from etl_project import etl_tests

    pd.DataFrame(etl_tests.launch()).to_json("etl_test_results.json", orient="records", indent=4)
    pd.DataFrame(dbt.launch_tests()).to_json("dbt_test_results.json", orient="records", indent=4)


def cmd_complexity(args):
    import complexity

    etl, elt = complexity.launch()
    with open("complexity_results.json", "w") as f:
        json.dump({"etl": etl, "elt": elt}, f, indent=2, default=str)


def cmd_lineage(args):
    import lineage

    etl, elt = lineage.launch()
    with open("lineage_results.json", "w") as f:
        json.dump({"etl": etl, "elt": elt}, f, indent=2, default=str)


def cmd_report(args):
    import report

    report.build()


def cmd_all(args):
    from scripts import dbgen

    if dbgen.count_parquet_tables() < len(dbgen.tables) or args.overwrite:
        cmd_generate(args)
    args.fractions = sorted(set(args.fractions) | {1.0})
    cmd_samples(args)
    cmd_etl(args)
    cmd_elt(args)
    # The databases now hold the last fraction (1.0): verify and test on the full dataset.
    cmd_verify(args)
    cmd_tests(args)
    cmd_complexity(args)
    cmd_lineage(args)
    cmd_report(args)


COMMANDS = {
    "generate": cmd_generate, "samples": cmd_samples, "etl": cmd_etl, "elt": cmd_elt,
    "verify": cmd_verify, "tests": cmd_tests, "complexity": cmd_complexity,
    "lineage": cmd_lineage, "report": cmd_report, "all": cmd_all,
}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", nargs="?", choices=COMMANDS)
    p.add_argument("--sf", type=float, default=1.0, help="TPC-H scale factor for `generate` (default 1)")
    p.add_argument("--fractions", type=float, nargs="+", default=SAMPLE_FRACTIONS,
                   help="sample fractions of orders to benchmark (default 0.1 0.5 1.0)")
    p.add_argument("--overwrite", action="store_true", help="regenerate existing TPC-H data")
    return p.parse_args(argv)


def interactive(args):
    menu = ["generate", "samples", "etl", "elt", "verify", "tests", "complexity", "lineage", "report", "all"]
    print("================ ETL vs ELT (dbt) ================\n")
    for i, name in enumerate(menu, 1):
        print(f"{i:>2}. {name}")
    print(f"{len(menu) + 1:>2}. quit\n")
    choice = input("Choice: ").strip()
    while not (choice.isdigit() and 1 <= int(choice) <= len(menu) + 1):
        choice = input(f"Enter a number between 1 and {len(menu) + 1}: ").strip()
    if int(choice) <= len(menu):
        COMMANDS[menu[int(choice) - 1]](args)


if __name__ == "__main__":
    args = parse_args()
    if args.command:
        COMMANDS[args.command](args)
    else:
        interactive(args)
