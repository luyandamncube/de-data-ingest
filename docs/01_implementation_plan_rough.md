
## Initial project plan

```text
 -> identify project deliverables
 -> break deliverables into atomic workloads
 -> "clickbench for python" to create test bench for our workloads
 -> for each workload, find the most efficient engine
 -> engine-specific execution : modular, assign engines to different workloads
 -> build pipelines 
```

## Initial project layout 

```text
pipeline/
├── ingest.py
├── transform.py
├── provision.py
├── stream_ingest.py
├── core/
├── fixtures/
├── plans/
├── scripts/
├── tests/
├── workloads/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── validation/
│   ├── dq/
│   └── stream/
```

| Area                                                                     | Purpose                                                                   |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| `pipeline/ingest.py`, `transform.py`, `provision.py`, `stream_ingest.py` | Keeps the official entrypoints exactly where the scorer expects them      |
| `pipeline/workloads/`                                                    | Holds the atomic runnable units                                           |
| `pipeline/plans/`                                                        | Combines atomic workloads into Stage 1 / 2 / 3 execution plans            |
| `pipeline/core/`                                                         | Shared runtime plumbing: Spark session, config, IO, schemas               |
| `pipeline/engines/`                                                      | Clean place to isolate engine-specific execution logic                    |
| `tests/`                                                                 | Lets you test helpers, atomic workloads, and end-to-end stages separately |
| `fixtures/`                                                              | Minimal datasets for each atomic workload                                 |
| `scripts/`                                                               | Developer-facing commands for running one workload or one stage           |
