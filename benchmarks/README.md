# Benchmark Suite Skeleton

This package holds the benchmark scaffolding introduced on
`feature/stage1-benchmark-suite`.

Current scope:

- canonical workload registry integration via `pipeline.registry`
- engine adapter contract
- benchmark result and matrix schema
- CLI skeleton for listing tracked units and emitting sample results

This branch intentionally does **not** implement Bronze, Silver, or Gold
runtime behavior yet. Those land on their dedicated Stage 1 branches.
