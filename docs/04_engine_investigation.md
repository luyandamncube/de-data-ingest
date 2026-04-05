## Engine Ranking 

Rank engines on two separate axes:

1. **Handoff cost to Delta**
How much extra conversion/materialization is needed before you can write valid Delta outputs.
2. **Thin-container efficiency**
How much of the 2 CPU / 2 GB budget is likely to go to your actual transforms, versus framework/runtime overhead.

This is a **challenge-specific ranking**, not a universal benchmark. 
- local files
- offline container
- local `/data/output`
- required Delta outputs.

## Ranking summary

| Engine | Delta handoff rank | Thin-container efficiency rank | Overall for your stated preference |
| --- | --- | --- | --- |
| **Polars** | **2** | **1** | **Best fit if you want to avoid Spark** |
| **Apache DataFusion** | **3** | **2** | **Best Rust/Arrow-heavy option** |
| **PySpark + Delta Lake** | **1** | **8** | **Safest path, highest framework overhead** |
| **PyArrow Dataset / Acero** | **4** | **4** | Very efficient, but more DIY and less turnkey |
| **DuckDB** | **5** | **3** | Great local engine, weaker Delta path |
| **ClickHouse Local** | **8** | **5** | Great local SQL speed, awkward local Delta output path |
| **Daft** | **7** | **6** | Promising local engine, weaker Delta story in the docs I checked |
| **Dask DataFrame** | **6** | **7** | Works, but pandas-partition model adds overhead here |

## Ranking AI Summary

## Why that ranking

| Engine | Why the Delta handoff is ranked this way | Why the thin-container efficiency is ranked this way |
| --- | --- | --- | 
| **PySpark + Delta Lake** | Spark can write Delta directly with `data.write.format("delta").save(...)`, so there is no engine boundary before the write. | Spark runs in local mode with `local[K]`, which is valid for a small container, but it still brings Spark/JVM-style runtime overhead compared with the embedded engines. |
| **Polars** | Polars has a direct `DataFrame.write_delta(...)` API, and its Arrow interchange is mostly zero-copy, so the Delta handoff is very clean. | Polars’ lazy `scan_*` path pushes optimizations into readers, and sinks can stream results to storage without holding everything in RAM. That is exactly the kind of behavior you want in a tight container. |
| **Apache DataFusion** | DataFusion exposes Arrow record-batch streams and zero-copy Arrow interchange; `delta-rs` accepts PyArrow tables and iterators of record batches, so the handoff to Delta can stay Arrow-native. | DataFusion is a Rust engine, runs in a multithreaded environment, and its Python docs explicitly call out zero-copy between Python and the engine. That is a strong fit for maximizing compute inside a thin container. |
| **PyArrow Dataset / Acero** | Arrow data can be written efficiently, and `delta-rs` accepts Arrow tables/record batches, so the path to Delta is still fairly direct. | Acero is built around execution plans over streams and Arrow batches, which is efficient, but it is lower-level and explicitly not a database or distributed engine, so you build more plumbing yourself. |
| **DuckDB** | DuckDB’s Delta extension is still experimental in the stable docs, and the LTS docs only mention limited write support; it also relies on the extension repository for install/load, which is not ideal for an offline challenge pipeline. | DuckDB is in-process, single-binary-ish, and designed to run embedded with zero external dependencies, so its local runtime efficiency is excellent. The issue is mostly the Delta path, not the execution engine. |
| **ClickHouse Local** | `clickhouse-local` is excellent on local files, but the current `DeltaLake` engine is aimed at **existing** Delta tables in S3/GCS/Azure, and writes require an experimental flag; that is a poor match for creating fresh local `/data/output/...` Delta tables in this challenge. | It is explicitly designed for local compute resources and local-file SQL without a server, so the execution side is still attractive. The weak spot is local Delta output, not raw compute. |
| **Dask DataFrame** | The natural write path is Parquet/CSV and pandas-like partitions; getting from there to Delta usually means another layer such as PyArrow or `delta-rs`. | Dask DataFrame is a collection of many pandas DataFrames, computation is triggered with `.compute()`, and the docs explicitly say you should probably stick to pandas when data is small or computation is already fast. In a 2 GB container, that usually means more overhead than Polars/DataFusion/DuckDB. |
| **Daft** | In the official Daft docs I checked, I found local CSV / JSON / Parquet reads and Parquet writes, but not a first-class Delta writer, so the Delta path looks indirect. | Daft can work on local files and write Parquet, which keeps it viable, but the documentation trail is less mature for this exact Delta-targeted use case. |