# Results

## End-to-end (extract + transform + load)

| Sample | Pipeline | Time (s) | CPU (s) | Peak memory (MB) |
|---|---|---|---|---|
| 0.1 | ETL (pandas) | 4.57 | 5.52 | 832 |
| 0.1 | ELT (dbt) | 7.89 | 11.79 | 577 |
| 0.5 | ETL (pandas) | 14.06 | 15.63 | 3,267 |
| 0.5 | ELT (dbt) | 9.39 | 25.51 | 948 |
| 1 | ETL (pandas) | 42.35 | 47.94 | 5,723 |
| 1 | ELT (dbt) | 7.88 | 26.98 | 903 |

## Transform time per model (sample 1)

| Model | Rows | ETL (pandas) s | ELT (dbt) s |
|---|---|---|---|
| dim_region | 5 | 0.004 | 0.274 |
| dim_date | 1,263 | 0.027 | 0.290 |
| dim_customer | 150,000 | 0.005 | 0.831 |
| dim_product | 200,000 | 0.005 | 0.848 |
| fact_sales | 2,996,217 | 10.777 | 0.576 |

dbt CLI start-up/parsing overhead: **1.24 s** per `dbt run`.

## Data-quality tests

| Pipeline | Passed | Failed | Time (s) |
|---|---|---|---|
| ETL (hand-written pandas checks) | 29 | 0 | 0.26 |
| ELT (`dbt test`, declared in YAML) | 29 | 0 | 3.97 |

## Development complexity

| Metric | ETL | ELT |
|---|---|---|
| Files | 3.0 | 14.0 |
| Source lines (SLOC) | 516.0 | 343.0 |
| Avg. cyclomatic complexity | 1.8 | 2.0 |
| Avg. maintainability index | 58.8 | 58.4 |
