## Branch naming pattern

```text
<type>/<stage-or-theme>-<short-scope>
```

## Branches
### Stage 1 branches

| Branch | Purpose |
| --- | --- |
| `feature/stage1-bronze-ingest` | Bronze raw ingest for 3 sources |
| `feature/stage1-silver-standardisation` | type casting, date parsing, dedup, linkage |
| `feature/stage1-gold-dimensional-model` | dims, fact, surrogate keys |
| `feature/stage1-validation-queries` | validation query execution and checks |

### Stage 2 branches

| Branch | Purpose |
| --- | --- |
| `feature/stage2-dq-framework` | DQ rules, flags, report generation |
| `feature/stage2-schema-evolution` | merchant_subcategory handling |
| `feature/stage2-scale-hardening` | performance and 3x-volume tuning |

### Stage 3 branches

| Branch | Purpose |
| --- | --- |
| `feature/stage3-stream-ingest` | polling, parse, replay protection |
| `feature/stage3-stream-state-tables` | current_balances and recent_transactions |
| `docs/stage3-adr` | ADR completion/refinement |