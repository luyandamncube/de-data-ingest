# Benchmark Suite Skeleton

This package holds the benchmark scaffolding introduced on
`feature/stage1-benchmark-suite`.

Current scope:

- canonical workload registry integration via `pipeline.registry`
- hybrid workload resolution scaffold for Python and SQL implementations
- engine adapter contract
- benchmark result and matrix schema
- host-side Docker orchestrator for sequential benchmark runs
- smoke workloads for both orchestration-only validation and real PySpark execution

## Mental model

The intended benchmark loop is:

1. the host expands a matrix of `engine x workload x attempts`
2. a thin Docker container is started for each run, sequentially
3. the container receives mounted input data and a mounted results directory
4. the selected engine adapter runs the requested workload
5. the host aggregates structured results and prints a summary table

Right now, the Docker loop is real and supports:

- `BM_06`: dummy orchestration validation
- `BM_07`: real engine-backed aggregation over `customers.csv`
- `BRZ_01`: PySpark Bronze customers ingest with Delta write/read validation

That lets us validate both the container loop and a genuine engine-backed run
before we implement real Bronze, Silver, or Gold workloads on their dedicated
branches.

## Hybrid workload scaffold

Workload codes stay stable while implementations can vary by engine. The
resolution order is:

1. engine-specific Python
2. engine-specific SQL
3. common Python
4. common SQL

The registry now carries an implementation bundle for each workload code, and
`benchmarks/sql/` is scaffolded for future SQL-backed workloads and overrides.
The current adapters still execute Python-backed implementations only; SQL
resolution is scaffolded but not wired yet.

## Docker smoke commands

Build the benchmark image:

```bash
python -m benchmarks.orchestrator docker-smoke \
  --build \
  --image de-data-ingest-bench:test \
  --data-dir "$(pwd)/data" \
  --engine pyspark_delta \
  --engine duckdb \
  --attempts 3
```

Run again without rebuilding:

```bash
python -m benchmarks.orchestrator docker-smoke \
  --image de-data-ingest-bench:test \
  --data-dir "$(pwd)/data" \
  --engine pyarrow_acero \
  --workload-id BM_06,BM_07 \
  --attempts 3
```

Workloads are executed sequentially in the order you pass them. You can also
repeat the flag instead of using commas:

```bash
python -m benchmarks.orchestrator docker-smoke \
  --image de-data-ingest-bench:test \
  --data-dir "$(pwd)/data" \
  --engine pyspark_delta \
  --workload-id BM_06 \
  --workload-id BM_07 \
  --attempts 3
```

## Notes

- The benchmark image is separate from the submission image on purpose.
- The Docker profile mirrors the challenge style: `--network=none`,
  `--memory=2g`, `--cpus=2`, `--read-only`, and `--tmpfs /tmp`.
- `BM_07` is intentionally small. It is a real engine-backed workload for
  `pyspark_delta`, `polars`, `datafusion`, `duckdb`, `clickhouse_local`, and
  `pyarrow_acero`, but it is still only a smoke benchmark, not a
  Bronze/Silver/Gold implementation.
- `BRZ_01` is the first real Bronze workload wired into the benchmark suite,
  currently implemented for `pyspark_delta` only.
- Real benchmark value arrives incrementally as executable workloads land on the
  Bronze, Silver, and Gold branches.
