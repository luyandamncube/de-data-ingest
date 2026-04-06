# SQL Asset Layout

This directory scaffolds the hybrid workload model for benchmark workloads.

Resolution intent per workload code:

1. engine-specific Python implementation
2. engine-specific SQL asset
3. common Python implementation
4. common SQL asset

That lets a single workload code stay stable while each engine resolves the
most suitable implementation it has available.

Directory roles:

- `common/`: shared SQL that may work across multiple engines
- `clickhouse/`: clickhouse-local specific SQL assets
- `spark/`: Spark-specific SQL overrides
- `duckdb/`: DuckDB-specific SQL overrides
- `datafusion/`: DataFusion-specific SQL overrides
- `polars/`: Polars SQL overrides

`pyarrow_acero` is intentionally omitted here because it is not naturally a
SQL-first engine; Acero workloads will usually remain Python-backed.
