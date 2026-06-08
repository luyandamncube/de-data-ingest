# Solution Defense Brief

## Executive Summary

This solution is best defended as a **Stage 1 batch pipeline delivered against a strict scorer contract**, with **Stage 2 and Stage 3 intentionally designed for extension** but not claimed here as fully complete deliverables.

The core submission keeps the scorer-facing interface simple:

- [`pipeline/run_all.py`](/home/mncubel/de-data-ingest/pipeline/run_all.py) is the single batch entrypoint
- [`config/pipeline_config.yaml`](/home/mncubel/de-data-ingest/config/pipeline_config.yaml) is the runtime contract for paths and engine routing
- `/data/output/bronze`, `/data/output/silver`, and `/data/output/gold` are written as Delta tables for downstream validation

From a judge's perspective, the main design choice was to separate the solution into **contract-stable pipeline stages** and **swappable execution engines**. That gave us a practical path to meet the challenge constraints first, while still leaving room to benchmark and evolve the implementation later.

## What Was Built

The implemented solution follows a medallion-style batch flow:

1. **Bronze ingestion**
   Reads the three raw source files, preserves source shape, adds one shared `ingestion_timestamp` for the run, and writes Delta outputs.
2. **Silver standardization**
   Standardizes types and dates, deduplicates by natural keys, validates account-to-customer linkage, and retains transaction quality issues through `dq_flag` instead of silently dropping records.
3. **Gold provisioning**
   Produces `dim_customers`, `dim_accounts`, and `fact_transactions` in the expected output shape with stable surrogate keys and scorer-aligned fields.

This is implemented in:

- [`pipeline/ingest.py`](/home/mncubel/de-data-ingest/pipeline/ingest.py)
- [`pipeline/transform.py`](/home/mncubel/de-data-ingest/pipeline/transform.py)
- [`pipeline/provision.py`](/home/mncubel/de-data-ingest/pipeline/provision.py)

## Why The Architecture Looks This Way

### 1. Medallion architecture was the safest way to satisfy the challenge contract

The challenge expects clear layer boundaries, Delta outputs at every layer, and scoreable Gold tables. A Bronze/Silver/Gold structure reduces ambiguity:

- Bronze is responsible for landing data reliably
- Silver is responsible for conformance and record-level quality handling
- Gold is responsible for business-facing analytics outputs

That separation makes it easier to defend correctness. If something fails, we know whether it is an ingestion issue, a conformance issue, or a modeling issue.

### 2. Workloads were decomposed into atomic units on purpose

The docs in [`docs/01_implementation_plan_rough.md`](/home/mncubel/de-data-ingest/docs/01_implementation_plan_rough.md), [`docs/02_workloads.md`](/home/mncubel/de-data-ingest/docs/02_workloads.md), and [`docs/05_workloads_full.md`](/home/mncubel/de-data-ingest/docs/05_workloads_full.md) show that the solution was planned as a set of small benchmarkable workloads rather than one monolithic script.

That choice matters because it gave us:

- traceability from challenge requirement to implementation unit
- a way to compare engines per workload instead of making one global engine bet too early
- a structure that can scale into Stage 2 data quality and Stage 3 streaming without rewriting the whole entrypoint

### 3. Engine routing is configurable because performance tradeoffs are workload-specific

The docs explicitly explored a hybrid model: benchmark workloads, then route each workload to the most suitable engine. That design survives in the implementation through config-driven adapter selection.

In the current code:

- Bronze adapters are selected in [`pipeline/bronze/factory.py`](/home/mncubel/de-data-ingest/pipeline/bronze/factory.py)
- Silver adapters are selected in [`pipeline/silver/factory.py`](/home/mncubel/de-data-ingest/pipeline/silver/factory.py)
- engine choices are declared in [`config/pipeline_config.yaml`](/home/mncubel/de-data-ingest/config/pipeline_config.yaml)

This lets the public interface stay stable even if the execution engine changes.

### 4. Delta was used as the contract format, not as an afterthought

The planning docs make a deliberate point that Bronze, Silver, and Gold should be treated as Delta tables because that is what the challenge contract and validation flow actually consume. That is why the solution writes Delta directly rather than treating Delta as a later conversion step.

This was important for two reasons:

- it aligns with the scorer's expectations
- it avoids building one pipeline for execution and a second pipeline for submission format

### 5. The checked-in implementation uses Polars deliberately

The design notes include an early recommendation to prefer PySpark for Bronze because Spark has a very direct Delta write path. That was a sensible early design default.

The final checked-in implementation, however, routes the current batch path through **Polars** for Bronze and Silver, and also uses Polars to build Gold from Delta-backed tables. That should be defended as a deliberate refinement, not a contradiction:

- the adapter architecture made engine swaps low-risk
- Polars is lightweight for a 2 CPU / 2 GB offline container
- the final code still preserves the scorer contract because the output remains Delta
- Spark support was retained as an available path for Bronze through the adapter layer, even though it is not the active default in the checked-in config

## Key, Partition, And Access Decisions

### 1. Natural keys are preserved until Gold

The pipeline keeps the business keys visible and useful through the early layers:

- `customers.customer_id` is the customer natural key
- `accounts.account_id` is the account natural key
- `accounts.customer_ref` is the business foreign key back to `customers.customer_id`
- `transactions.transaction_id` is the transaction natural key

That matters because Silver can deduplicate and validate relationships using source-aligned keys before any warehouse-style surrogate keys are introduced.

### 2. Surrogate keys are introduced only in Gold

The Gold layer generates:

- `customer_sk` for `dim_customers`
- `account_sk` for `dim_accounts`
- `transaction_sk` for `fact_transactions`

In the current implementation, these are created by sorting on the natural key and then assigning row numbers. That is a deliberate choice because it keeps the keys:

- unique
- non-null
- stable across reruns on the same input data

This is exactly the kind of key strategy judges usually want to hear: preserve business meaning where it helps, then introduce surrogate keys only where dimensional joins and scored output require them.

### 3. Referential keys are enforced explicitly

The most important relationship decisions are:

- `accounts.customer_ref` must resolve to `customers.customer_id`
- Gold `dim_accounts.customer_id` is a semantic rename of `customer_ref`
- `fact_transactions.account_sk` is resolved from `account_id`
- `fact_transactions.customer_sk` is resolved through the account-to-customer relationship

This gives the model a clean dimensional shape while still remaining traceable back to the source system keys.

### 4. Physical partitioning was kept simple and contract-driven

The challenge brief expects Bronze to be partitioned by source. In this implementation, that is satisfied at the table-directory level:

- `/data/output/bronze/customers`
- `/data/output/bronze/accounts`
- `/data/output/bronze/transactions`

In other words, each source lands in its own Delta table rather than all records being mixed into one shared Bronze dataset with an extra partition column.

That is a reasonable defense in a hackathon setting because:

- it matches the source-oriented contract cleanly
- it keeps validation straightforward
- it avoids adding unnecessary physical complexity early

### 5. We did not depend on database-style indexing

The checked-in solution does **not** implement a separate indexing layer in the database sense, and that is worth stating directly.

This repo is built around local file-backed Delta tables plus Polars scans, so the main access strategy is:

- narrow table boundaries
- deterministic sorts where key stability matters
- direct joins on natural and surrogate keys

That was a deliberate tradeoff. Under a 2 CPU / 2 GB offline container constraint, the priority was correctness, portability, and predictable file-based execution rather than introducing an indexing subsystem that the scorer does not require.

If asked about this in a defense, the concise answer is: **we optimized the data model and access paths first, rather than adding database-style indexing that would not materially improve a small local scored run.**

## How Correctness Is Enforced

The solution does not rely only on “the code looks right.” It includes explicit contract checks.

### Bronze contract validation

[`pipeline/gates/bronze_validation.py`](/home/mncubel/de-data-ingest/pipeline/gates/bronze_validation.py) verifies that:

- each Bronze output path exists
- each Bronze table is a real Delta table
- row counts match the landed source files
- schemas match the expected Bronze contract
- each table has exactly one run-level `ingestion_timestamp`
- all Bronze tables share the same ingestion timestamp for the run

This is a strong defense point because it proves Bronze correctness at the contract level, not just at the code level.

### Silver quality handling

[`pipeline/silver/polars_adapter.py`](/home/mncubel/de-data-ingest/pipeline/silver/polars_adapter.py) handles:

- mixed-format date parsing
- currency normalization toward `ZAR`
- deduplication by business key using latest ingestion timestamp
- account-to-customer linkage validation
- transaction-level `dq_flag` assignment for implemented issue classes

The important design decision here is that suspect records are generally **retained and flagged**, not invisibly discarded. That keeps the pipeline auditable.

### Gold output discipline

[`pipeline/provision.py`](/home/mncubel/de-data-ingest/pipeline/provision.py) creates the scorer-facing dimensional model and uses deterministic ordering on natural keys before generating surrogate keys. That keeps outputs stable across reruns on the same input.

## Delivery Status

### Stage 1

This is the defensible delivered baseline.

- batch entrypoint implemented
- Bronze, Silver, Gold outputs implemented
- Delta outputs produced at each layer
- validation-oriented Gold model implemented
- strong Bronze contract validation included

### Stage 2

This should be described as **partially prepared and partially reflected in code**, not fully delivered.

What is already visible in code:

- optional `merchant_subcategory` handling
- transaction `dq_flag` population for some issue categories
- DQ summary report generation in Gold when configured

What should **not** be claimed as complete:

- full externalized DQ rule execution from [`config/dq_rules.yaml`](/home/mncubel/de-data-ingest/config/dq_rules.yaml)
- full Stage 2 acceptance coverage across all documented issue types

### Stage 3

This should be described as **architecturally anticipated but not implemented**.

- [`pipeline/stream_ingest.py`](/home/mncubel/de-data-ingest/pipeline/stream_ingest.py) is still a scaffold
- [`adr/stage3_adr.md`](/home/mncubel/de-data-ingest/adr/stage3_adr.md) is still a template
- the docs clearly show the intended streaming direction, but the checked-in code should not be presented as a finished streaming delivery

## Judge FAQ

### Why did you choose a medallion architecture?

Because the challenge is naturally staged. Bronze is raw landing, Silver is standardization and data quality handling, and Gold is the scoreable business model. That separation reduces risk and makes validation easier because each layer has a narrow responsibility.

### Why did you break the work into atomic workloads instead of just building one pipeline?

Because the challenge combines correctness, performance, and changing requirements. Atomic workloads let us map each official deliverable to a tractable unit, benchmark them independently, and evolve the implementation without destabilizing the entire submission.

### Why make engine routing configurable?

Because engine choice is not one-size-fits-all. Some workloads favor direct Delta integration, while others favor lighter in-memory execution. Configurable routing keeps the external contract stable while allowing internal optimization.

### Why Delta at every layer?

Because Delta is part of the actual contract, not just a storage preference. Using Delta end-to-end keeps the implementation aligned with the scorer and avoids introducing a risky “convert-to-submission-format later” step.

### How did you think about keys?

We kept natural keys visible where they carry business meaning and used them for deduplication and relationship checks. Then, in Gold, we introduced stable surrogate keys for the dimensional model. That gives us both traceability back to source records and scorer-friendly warehouse joins.

### Why didn’t you generate surrogate keys earlier?

Because early layers are easier to validate and debug when they still speak in source-system keys. Surrogate keys are most useful at the dimensional output boundary, so that is where we introduced them.

### What was your partitioning strategy?

We kept physical partitioning simple. Bronze is separated by source into distinct Delta table directories, which is the cleanest interpretation of “partitioned by source” for this challenge. We did not add extra partition-column complexity where it was not needed for scoring or correctness.

### What about indexing?

We did not build a separate indexing mechanism. This is a local file-based pipeline, not a long-running serving database. The practical performance decisions were engine choice, lazy scans, table boundaries, and deterministic key-based joins rather than maintaining secondary indexes.

### Why is Polars the active engine in the final checked-in path?

Because the final code path is optimized for a tight local container. Polars offers a lightweight execution model while still preserving the required Delta outputs. The adapter design made it possible to move to that lighter-weight default without changing the public pipeline interface.

### If the docs recommended Spark for Bronze early on, why didn’t you keep that as the final default?

The early Spark recommendation was part of an exploration phase focused on Delta handoff safety. The final implementation benefitted from the benchmark-driven architecture: once the adapter boundary existed, we could keep Spark available as an option and still choose a lighter default for the checked-in configuration.

### How do you know the solution is correct?

Because correctness is checked in layers. Bronze has explicit contract validation for row counts, schema shape, Delta structure, and run-level timestamps. Silver performs conformance and linkage validation. Gold is shaped to the scorer-facing schema and validation queries.

### How do you handle bad data without hiding it?

The solution’s direction is to retain problematic transaction records where possible and mark them with `dq_flag`. That is a better audit posture than silently dropping data because it preserves both analytical continuity and visibility into quality issues.

### How do you handle schema drift?

The clearest current example is `merchant_subcategory`: the implementation tolerates it being absent and surfaces it when present. That shows the intended extension pattern for later-stage schema evolution, even though the full Stage 2 framework is not yet complete.

### How does the solution stay within the 2 CPU / 2 GB / offline constraints?

The design is intentionally local and self-contained: file-based inputs, no network dependency during runtime, config-driven local paths, and a lightweight active engine choice. The architecture also avoids introducing unnecessary conversion stages that would increase memory and IO overhead.

### Why include Stage 2 and Stage 3 in the story if Stage 1 is the delivered baseline?

Because a strong hackathon defense should show not only that the current solution works, but that the architecture was chosen with extension in mind. The key is to present later stages honestly as design direction and partial preparation, not as completed delivery.

### What would you improve next if you had more time?

The next improvements are clear:

- fully externalize DQ rule execution from `dq_rules.yaml`
- complete Stage 2 acceptance coverage across all documented issue classes
- implement the Stage 3 streaming path and ADR
- add deeper automated checks around Silver and Gold contracts, not only Bronze

## Closing Position

The best defense of this repo is not “we solved every future stage already.” It is:

**we delivered a credible Stage 1 batch pipeline with strong contract discipline, designed it so engines can evolve without breaking the scorer interface, and left visible extension points for Stage 2 and Stage 3 rather than painting ourselves into a corner.**

That is a pragmatic and defensible engineering story for a hackathon setting.
