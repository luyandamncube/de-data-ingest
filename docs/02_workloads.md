# Composite Mapping: Official Deliverables -> Internal Tracking Units

This document is the canonical traceability map between the challenge pack and
our internal execution model.

## Unit roles

| Role | Meaning |
| --- | --- |
| `workload` | Executable, benchmarkable unit of work |
| `contract` | Interface or output invariant that workloads must satisfy |
| `gate` | Acceptance check used to validate a branch or stage |
| `scenario` | Stress, replay, SLA, or coexistence exercise |
| `benchmark_support` | Internal benchmark-platform work, not a challenge deliverable |

## Internal benchmark platform (`BM_*`)

| Code | Role | Purpose | Tracking Unit ID |
| --- | --- | --- | --- |
| `BM_01` | `benchmark_support` | Canonical workload registry and manifest | `bm_01_workload_registry` |
| `BM_02` | `benchmark_support` | Benchmark runner skeleton | `bm_02_benchmark_runner` |
| `BM_03` | `benchmark_support` | Engine adapter contract | `bm_03_engine_adapter_contract` |
| `BM_04` | `benchmark_support` | Result matrix and report schema | `bm_04_result_matrix_schema` |
| `BM_05` | `benchmark_support` | Workload catalog normalization support | `bm_05_catalog_normalisation` |
| `BM_06` | `benchmark_support` | Dummy Docker smoke loop for benchmark orchestration | `bm_06_docker_smoke_validation` |
| `BM_07` | `benchmark_support` | Real PySpark aggregation over `customers.csv` for smoke validation | `bm_07_customers_pyspark_aggregation` |

## Stage 1 executable workloads

### Bronze (`BRZ_*`)

| Code | Official deliverable | Tracking Unit ID |
| --- | --- | --- |
| `BRZ_01` | Ingest `customers.csv` to Bronze | `s1_b01_ingest_customers_raw` |
| `BRZ_02` | Ingest `accounts.csv` to Bronze | `s1_b02_ingest_accounts_raw` |
| `BRZ_03` | Ingest `transactions.jsonl` to Bronze | `s1_b03_ingest_transactions_raw` |
| `BRZ_04` | Add one consistent `ingestion_timestamp` per ingestion run | `s1_b04_run_ingestion_timestamp` |
| `BRZ_05` | Write Bronze as Delta format | `s1_b05_bronze_delta_write` |

### Silver (`SLV_*`)

| Code | Official deliverable | Tracking Unit ID |
| --- | --- | --- |
| `SLV_01` | Standardise customers types and date formats | `s1_s01_customers_cast_dates` |
| `SLV_02` | Standardise accounts types and date formats | `s1_s02_accounts_cast_dates` |
| `SLV_03` | Standardise transactions types and date formats | `s1_s03_transactions_parse_standardise` |
| `SLV_04` | Deduplicate customers on primary key | `s1_s04_dedup_customers_pk` |
| `SLV_05` | Deduplicate accounts on primary key | `s1_s05_dedup_accounts_pk` |
| `SLV_06` | Deduplicate transactions on primary key | `s1_s06_dedup_transactions_pk` |
| `SLV_07` | Enforce standard currency representation | `s1_s07_currency_standardise` |
| `SLV_08` | Resolve account-to-customer linkage | `s1_s08_account_customer_linkage` |

### Gold (`GLD_*`)

| Code | Official deliverable | Tracking Unit ID |
| --- | --- | --- |
| `GLD_01` | Build `dim_customers` | `s1_g01_dim_customers_build` |
| `GLD_02` | Build `dim_accounts` | `s1_g02_dim_accounts_build` |
| `GLD_03` | Build `fact_transactions` | `s1_g03_fact_transactions_build` |
| `GLD_04` | Generate stable surrogate keys | `s1_g04_surrogate_keys_stable` |

## Stage 1 contracts

| Code | Contract | Tracking Unit ID |
| --- | --- | --- |
| `CTR_01` | Bronze layer contract: authoritative paths, Delta output, run-level timestamp | `s1_c01_bronze_contract` |
| `CTR_02` | Silver layer contract: typed, deduplicated, link-ready outputs | `s1_c02_silver_contract` |
| `CTR_03` | Gold layer contract: dimensional schema, foreign keys, stable surrogate keys | `s1_c03_gold_contract` |

## Stage 1 acceptance gates

These are acceptance gates, not executable workloads.

| Code | Gate | Tracking Unit ID |
| --- | --- | --- |
| `VAL_01` | Pass validation query: transaction volume by type | `s1_v01_txn_volume_by_type` |
| `VAL_02` | Pass validation query: zero unlinked accounts | `s1_v02_zero_unlinked_accounts` |
| `VAL_03` | Pass validation query: province distribution | `s1_v03_province_distribution` |

## Stage 2 executable workloads

| Code | Official deliverable | Tracking Unit ID |
| --- | --- | --- |
| `S2_SE_01` | Accept new `merchant_subcategory` field in transactions | `s2_se01_merchant_subcategory_optional` |
| `S2_DQ_01` | Handle duplicate records via dedup logic | `s2_dq01_duplicate_deduped` |
| `S2_DQ_02` | Detect and handle orphaned account references | `s2_dq02_orphaned_account` |
| `S2_DQ_03` | Detect and handle type mismatch issues | `s2_dq03_type_mismatch_amount` |
| `S2_DQ_04` | Detect and handle inconsistent date formats | `s2_dq04_date_format_normalise` |
| `S2_DQ_05` | Detect and handle currency variants | `s2_dq05_currency_variant_normalise` |
| `S2_DQ_06` | Detect and handle null required fields | `s2_dq06_null_required_account_id` |
| `S2_DQ_08` | Produce DQ report JSON output | `s2_dq08_dq_report_build` |

## Stage 2 contracts

| Code | Contract | Tracking Unit ID |
| --- | --- | --- |
| `S2_DQ_07` | Externalise DQ handling rules in `config/dq_rules.yaml` | `s2_dq07_rules_externalised` |

## Stage 2 acceptance gates

| Code | Gate | Tracking Unit ID |
| --- | --- | --- |
| `S2_VAL_01` | Still pass validation queries under Stage 2 conditions | `s2_v01_stage2_validation_queries` |

## Stage 2 scenarios

| Code | Scenario | Tracking Unit ID |
| --- | --- | --- |
| `S2_SC_01` | Handle 3x data volume within constraints | `s2_sc01_volume_3x_smoke` |

## Stage 3 executable workloads

| Code | Official deliverable | Tracking Unit ID |
| --- | --- | --- |
| `S3_ST_01` | Poll stream directory and process files in order | `s3_st01_stream_discovery_order` |
| `S3_ST_02` | Parse and standardise stream transactions | `s3_st02_stream_parse_normalise` |
| `S3_ST_03` | Maintain `current_balances` table | `s3_st03_current_balances_merge` |
| `S3_ST_04` | Maintain `recent_transactions` table | `s3_st04_recent_transactions_top50` |

## Stage 3 scenarios

| Code | Scenario | Tracking Unit ID |
| --- | --- | --- |
| `S3_ST_05` | Enforce `updated_at` SLA | `s3_st05_stream_updated_at_sla` |
| `S3_ST_06` | Keep batch and stream paths working together | `s3_st06_stream_batch_coexistence` |
| `S3_ST_07` | Avoid reprocessing already handled stream files | `s3_st07_stream_replay_safety` |

## Stage 3 gates

| Code | Gate | Tracking Unit ID |
| --- | --- | --- |
| `S3_ADR_01` | Submit `adr/stage3_adr.md` | `s3_adr_stage3_submission` |
