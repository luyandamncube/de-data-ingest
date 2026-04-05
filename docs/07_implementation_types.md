# Implementation Types

This note captures the hybrid workload model for the benchmark suite.

## Goal

Keep the public benchmark interface stable around a single workload code, while
allowing different engines to run that workload through the implementation type
that suits them best.

Example:

- run command uses `--workload-id SLV_03`
- registry resolves the best implementation for each selected engine
- adapters execute the engine-specific interpretation behind the same workload code

## Core implementation types

| Type | Meaning | Typical use |
| --- | --- | --- |
| `common_sql` | Shared SQL asset usable across multiple engines | scans, filters, joins, aggregations, validation queries |
| `engine_sql` | SQL override for one engine | syntax differences, engine-specific functions, SQL dialect fixes |
| `common_python` | Shared Python callable | lightweight glue, fallback logic, portable prototype workloads |
| `engine_python` | Python implementation for one engine | Spark jobs, Polars LazyFrame logic, Dask/Daft workloads |

## Likely future extensions

| Type | Meaning | Typical use |
| --- | --- | --- |
| `engine_cli_sql` | SQL executed via CLI or subprocess | ClickHouse Local |
| `engine_plan` | Non-SQL execution plan / expression graph | PyArrow Dataset / Acero |

## Engine fit

| Engine | Natural implementation types |
| --- | --- |
| `pyspark_delta` | `engine_sql`, `engine_python`, sometimes `common_sql` |
| `duckdb` | `engine_sql`, `common_sql` |
| `polars` | `engine_python`, `engine_sql`, sometimes `common_sql` |
| `datafusion` | `engine_sql`, `common_sql`, sometimes `engine_python` |
| `pyarrow_acero` | `engine_plan`, `engine_python` |
| `clickhouse_local` | `engine_cli_sql`, `engine_sql` |
| `dask` | `engine_python` |
| `daft` | `engine_python`, sometimes `engine_sql` |

## Resolution rule

The benchmark suite should resolve implementations in this order:

1. engine-specific Python
2. engine-specific SQL
3. common Python
4. common SQL

This keeps the workload code stable while still allowing specialized overrides.

## Practical guidance

- Prefer `common_sql` when a workload is naturally relational and portable.
- Prefer `engine_python` when the engine is DataFrame-first or plan-first.
- Do not force every engine into SQL.
- Do not force every workload into Python if SQL is the natural expression.
- Use overrides only when the common implementation stops being clean or correct.
