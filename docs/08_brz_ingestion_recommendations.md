# Bronze Ingestion AI Recommendations

## Contract first

Stage 1 Bronze outputs should be treated as **Delta tables**, not plain
Parquet-only intermediates.

This is the stronger interpretation of the challenge pack:

- `challenge_brief.md`: Bronze, Silver, and Gold outputs are Delta Parquet
- `output_schema_spec.md`: all layers are Delta Parquet
- `docker_interface_contract.md`: `/data/output/bronze/` is Bronze layer Delta tables
- `validation_queries.sql`: scoring reads via Delta format

`README_DOCKER.md` contains some looser Parquet wording, but it conflicts with
the more specific schema/interface/validation documents and should be treated
as stale.

## Recommended Stage 1 route

For Bronze, the most efficient default is:

1. read raw source from mounted CSV / JSONL
2. apply explicit schema
3. add one shared run-level `ingestion_timestamp`
4. write Bronze directly as Delta

Do **not** introduce a raw -> Parquet -> Delta step for the official Bronze
layer unless benchmarking shows an overwhelming benefit. It adds an extra
materialization boundary without helping the acceptance contract.

## Engine recommendation

### Default submission path

Use **PySpark + Delta** for all Stage 1 Bronze workloads.

Reason:

- direct CSV and JSONL readers
- natural handling of nested `transactions.jsonl`
- direct Delta writes with no handoff layer
- already present in the provided base image

### Bronze workload routing

| Workload | Recommended route |
| --- | --- |
| `BRZ_01` customers ingest | PySpark CSV -> add timestamp -> Delta |
| `BRZ_02` accounts ingest | PySpark CSV -> add timestamp -> Delta |
| `BRZ_03` transactions ingest | PySpark JSONL -> add timestamp -> Delta |
| `BRZ_04` run timestamp | Generate once per pipeline run and reuse |
| `BRZ_05` Bronze Delta write | Keep in the same engine as ingest |

## Where benchmarking still makes sense

Bronze should be benchmarked as **end-to-end source ingest workloads**, not
just as isolated operators.

Recommended Bronze benchmark units:

- `customers.csv -> Bronze Delta`
- `accounts.csv -> Bronze Delta`
- `transactions.jsonl -> Bronze Delta`

This is more realistic than benchmarking:

- timestamp add alone
- Delta write alone
- CSV parse alone

## Hybrid options

If we benchmark alternatives, the most promising comparison is:

- `customers.csv`: PySpark vs Polars
- `accounts.csv`: PySpark vs Polars
- `transactions.jsonl`: PySpark first, others later only if clearly justified

Even if another engine wins raw scan speed, PySpark may still remain the better
Bronze choice because the Bronze contract is **raw source -> Delta table**, not
raw source -> in-memory transform only.

## Practical conclusion

For Stage 1 implementation:

- build Bronze first with Spark-only direct Delta writes
- keep Bronze benchmarking separate from Silver/Gold operator benchmarking
- use Parquet-prepared benchmark inputs later for transform-heavy workloads, not
  for the official Bronze output path
