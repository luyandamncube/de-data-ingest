## Branch naming pattern

```text
<type>/<stage-or-theme>-<short-scope>
```

## Branch ownership rules

- Each branch owns a narrow code family.
- Each branch is expected to satisfy its defined validation gate before merge.
- `main` is integration only; feature work lands on the stage-specific branches below.

## Stage 1 branches

| Order | Branch | Primary codes | Purpose | Validation gate |
| --- | --- | --- | --- | --- |
| 1 | `feature/stage1-benchmark-suite` | `BM_*` | Benchmark platform, workload registry, runner skeleton, engine adapter contract, result matrix shape | Benchmark smoke, valid registry load, valid sample result emission, dummy Docker smoke loop |
| 2 | `feature/stage1-bronze-ingest` | `BRZ_*`, `CTR_01` | Bronze raw ingest for 3 sources, run-level timestamp, Bronze Delta writes | Container builds, Bronze outputs created, Bronze Delta tables readable |
| 3 | `feature/stage1-silver-standardisation` | `SLV_*`, `CTR_02` | Type casting, date parsing, dedup, linkage | Bronze+Silver smoke passes, targeted checks for casts, dedup, linkage |
| 4 | `feature/stage1-gold-dimensional-model` | `GLD_*`, `CTR_03` | Dims, fact, surrogate keys | First full Stage 1 end-to-end run, Gold Delta tables readable |
| 5 | `feature/stage1-validation-queries` | `VAL_*` | Validation query execution helpers and scorer-aligned final checks | Full `run_tests.sh --stage 1` harness passes |

## Stage 2 branches

| Branch | Primary codes | Purpose |
| --- | --- | --- |
| `feature/stage2-dq-framework` | `S2_DQ_*` | DQ rules, flags, report generation |
| `feature/stage2-schema-evolution` | `S2_SE_*` | `merchant_subcategory` handling |
| `feature/stage2-scale-hardening` | `S2_SC_*`, `S2_VAL_*` | Performance and 3x-volume tuning |

## Stage 3 branches

| Branch | Primary codes | Purpose |
| --- | --- | --- |
| `feature/stage3-stream-ingest` | `S3_ST_01`, `S3_ST_02`, `S3_ST_06`, `S3_ST_07` | Polling, parse, replay protection, batch/stream coexistence |
| `feature/stage3-stream-state-tables` | `S3_ST_03`, `S3_ST_04`, `S3_ST_05` | `current_balances`, `recent_transactions`, SLA-aligned updates |
| `docs/stage3-adr` | `S3_ADR_01` | ADR completion and refinement |
