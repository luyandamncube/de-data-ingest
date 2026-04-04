### Stage 1 — batch pipeline

These are the **real required workloads/deliverables** from the pack:

| Stage | Explicit requirement | Required |
| --- | --- | --- |
| 1 | Ingest `accounts.csv` to Bronze | Yes |
| 1 | Ingest `customers.csv` to Bronze | Yes |
| 1 | Ingest `transactions.jsonl` to Bronze | Yes |
| 1 | Add one consistent `ingestion_timestamp` per ingestion run | Yes |
| 1 | Write Bronze as Delta format | Yes |
| 1 | Partition Bronze by source | Yes |
| 1 | Transform Bronze → Silver | Yes |
| 1 | Deduplicate on natural keys | Yes |
| 1 | Standardise types | Yes |
| 1 | Standardise date formats | Yes |
| 1 | Resolve account-to-customer linkage | Yes |
| 1 | Enforce schema | Yes |
| 1 | Build `dim_customers` | Yes |
| 1 | Build `dim_accounts` | Yes |
| 1 | Build `fact_transactions` | Yes |
| 1 | Generate stable surrogate keys | Yes |
| 1 | Derive `age_band` from `dob` | Yes |
| 1 | Rename `accounts.customer_ref` → `dim_accounts.customer_id` | Yes |
| 1 | Pass validation query 1 | Yes |
| 1 | Pass validation query 2 | Yes |
| 1 | Pass validation query 3 | Yes |

### Stage 2 — stress + DQ + schema evolution

These are also **explicit**:

| Stage | Explicit requirement | Required |
| --- | --- | --- |
| 2 | Handle 3× volume | Yes |
| 2 | Handle `merchant_subcategory` field in transactions | Yes |
| 2 | Remain backward-compatible with Stage 1 data where that field is absent | Yes |
| 2 | Detect/handle `DUPLICATE_DEDUPED` | Yes |
| 2 | Detect/handle `ORPHANED_ACCOUNT` | Yes |
| 2 | Detect/handle `TYPE_MISMATCH` | Yes |
| 2 | Detect/handle `DATE_FORMAT` | Yes |
| 2 | Detect/handle `CURRENCY_VARIANT` | Yes |
| 2 | Detect/handle `NULL_REQUIRED` | Yes |
| 2 | Do not silently drop records | Yes |
| 2 | Externalise DQ rules in `config/dq_rules.yaml` | Yes |
| 2 | Set `fact_transactions.dq_flag` from the allowed codes | Yes |
| 2 | Produce `/data/output/dq_report.json` | Yes |
| 2 | Still pass the validation queries | Yes |

### Stage 3 — streaming extension

These are the **actual Stage 3 requirements**:

| Stage | Explicit requirement | Required |
| --- | --- | --- |
| 3 | Keep Stage 2 batch pipeline working | Yes |
| 3 | Poll `/data/stream/` | Yes |
| 3 | Process 12 JSONL stream files in filename order | Yes |
| 3 | Avoid reprocessing already handled files | Yes |
| 3 | Build/update `current_balances` | Yes |
| 3 | Build/update `recent_transactions` | Yes |
| 3 | Keep only the last 50 transactions per account | Yes |
| 3 | Meet the `updated_at` SLA against event timestamp | Yes |
| 3 | Write stream outputs in Delta format | Yes |
| 3 | Submit `adr/stage3_adr.md` | Yes |