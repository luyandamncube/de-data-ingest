"""
Canonical tracking registry for benchmark support work, workloads, contracts,
gates, and scenarios.

The benchmark-suite branch keeps this module declarative on purpose: it gives
us one place to trace planning codes, branch ownership, and future workload
families without coupling the current branch to concrete Stage 1 pipeline
implementations yet.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum


SHORTLIST_ENGINES = (
    "pyspark_delta",
    "polars",
    "datafusion",
    "duckdb",
    "clickhouse_local",
    "pyarrow_acero",
)


class TrackingRole(StrEnum):
    BENCHMARK_SUPPORT = "benchmark_support"
    WORKLOAD = "workload"
    CONTRACT = "contract"
    GATE = "gate"
    SCENARIO = "scenario"


@dataclass(frozen=True, slots=True)
class ImplementationBundle:
    """Hybrid implementation references for one workload code.

    Resolution order is handled outside the registry. The registry stays purely
    declarative so one workload code can later fan out to Python and/or SQL
    implementations depending on the selected engine.
    """

    common_python_ref: str | None = None
    common_sql_ref: str | None = None
    engine_python_refs: dict[str, str] = field(default_factory=dict)
    engine_sql_refs: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "common_python_ref": self.common_python_ref,
            "common_sql_ref": self.common_sql_ref,
            "engine_python_refs": dict(self.engine_python_refs),
            "engine_sql_refs": dict(self.engine_sql_refs),
        }


@dataclass(frozen=True, slots=True)
class TrackingUnit:
    id: str
    stage: str
    role: TrackingRole
    branch_owner: str
    parent_requirement: str
    family: str
    variant: str
    candidate_engines: tuple[str, ...]
    validation_gate: str
    description: str
    implementations: ImplementationBundle = field(
        default_factory=ImplementationBundle
    )

    def as_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["role"] = self.role.value
        data["candidate_engines"] = list(self.candidate_engines)
        data["implementations"] = self.implementations.as_dict()
        return data


TRACKING_MANIFEST: tuple[TrackingUnit, ...] = (
    TrackingUnit(
        id="BM_01",
        stage="stage1",
        role=TrackingRole.BENCHMARK_SUPPORT,
        branch_owner="feature/stage1-benchmark-suite",
        parent_requirement="Controlled benchmark platform for Stage 1",
        family="benchmark_platform",
        variant="workload_registry",
        candidate_engines=(),
        validation_gate="Registry loads and filters by role/stage",
        description="Canonical workload registry and manifest.",
    ),
    TrackingUnit(
        id="BM_02",
        stage="stage1",
        role=TrackingRole.BENCHMARK_SUPPORT,
        branch_owner="feature/stage1-benchmark-suite",
        parent_requirement="Controlled benchmark platform for Stage 1",
        family="benchmark_platform",
        variant="runner_skeleton",
        candidate_engines=(),
        validation_gate="Runner lists tracked units without importing runtime workload modules",
        description="Benchmark runner skeleton.",
    ),
    TrackingUnit(
        id="BM_03",
        stage="stage1",
        role=TrackingRole.BENCHMARK_SUPPORT,
        branch_owner="feature/stage1-benchmark-suite",
        parent_requirement="Controlled benchmark platform for Stage 1",
        family="benchmark_platform",
        variant="engine_adapter_contract",
        candidate_engines=(),
        validation_gate="Adapter contract can describe supported workload families",
        description="Engine adapter contract for shortlisted engines.",
    ),
    TrackingUnit(
        id="BM_04",
        stage="stage1",
        role=TrackingRole.BENCHMARK_SUPPORT,
        branch_owner="feature/stage1-benchmark-suite",
        parent_requirement="Controlled benchmark platform for Stage 1",
        family="benchmark_platform",
        variant="result_matrix_schema",
        candidate_engines=(),
        validation_gate="Sample benchmark result serializes cleanly",
        description="Benchmark result matrix and report schema.",
    ),
    TrackingUnit(
        id="BM_05",
        stage="stage1",
        role=TrackingRole.BENCHMARK_SUPPORT,
        branch_owner="feature/stage1-benchmark-suite",
        parent_requirement="Controlled benchmark platform for Stage 1",
        family="benchmark_platform",
        variant="catalog_normalisation",
        candidate_engines=(),
        validation_gate="Docs and registry stay aligned on roles and ownership",
        description="Workload catalog normalization support.",
    ),
    TrackingUnit(
        id="BM_06",
        stage="stage1",
        role=TrackingRole.BENCHMARK_SUPPORT,
        branch_owner="feature/stage1-benchmark-suite",
        parent_requirement="Controlled benchmark platform for Stage 1",
        family="docker_smoke",
        variant="container_loop_validation",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Host orchestrator can build an image, run sequential containers, and emit result records",
        description="Dummy Docker smoke workload for validating the benchmark loop.",
        implementations=ImplementationBundle(
            common_python_ref="benchmarks.workloads.smoke.bm_06_docker_smoke:run"
        ),
    ),
    TrackingUnit(
        id="BM_07",
        stage="stage1",
        role=TrackingRole.BENCHMARK_SUPPORT,
        branch_owner="feature/stage1-benchmark-suite",
        parent_requirement="Controlled benchmark platform for Stage 1",
        family="docker_smoke",
        variant="pyspark_customers_aggregation",
        candidate_engines=(
            "pyspark_delta",
            "polars",
            "datafusion",
            "duckdb",
            "clickhouse_local",
            "pyarrow_acero",
        ),
        validation_gate=(
            "Host orchestrator can execute a real engine-backed workload over "
            "customers.csv and emit a result record"
        ),
        description=(
            "Real engine-backed smoke workload that aggregates the mounted "
            "customers dataset."
        ),
        implementations=ImplementationBundle(
            common_python_ref=(
                "benchmarks.workloads.smoke."
                "bm_07_customers_pyspark_aggregation:run"
            ),
            engine_sql_refs={
                "polars": "benchmarks/sql/common/bm_07_customers_aggregation.sql",
                "datafusion": "benchmarks/sql/common/bm_07_customers_aggregation.sql",
                "duckdb": "benchmarks/sql/common/bm_07_customers_aggregation.sql",
                "clickhouse_local": (
                    "benchmarks/sql/clickhouse/"
                    "bm_07_customers_aggregation.sql"
                ),
            },
            engine_python_refs={
                "pyarrow_acero": (
                    "benchmarks.workloads.smoke."
                    "bm_07_customers_pyarrow_aggregation:run"
                )
            },
            common_sql_ref="benchmarks/sql/common/bm_07_customers_aggregation.sql",
        ),
    ),
    TrackingUnit(
        id="BRZ_01",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-bronze-ingest",
        parent_requirement="Ingest customers.csv to Bronze",
        family="bronze_ingest",
        variant="customers_csv_raw",
        candidate_engines=("pyspark_delta", "polars", "pyarrow_acero"),
        validation_gate="Bronze customers Delta table created and readable",
        description="Raw customers CSV ingest with explicit schema application.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "pyspark_delta": (
                    "benchmarks.workloads.bronze."
                    "brz_01_ingest_customers_raw:run"
                ),
                "polars": (
                    "benchmarks.workloads.bronze."
                    "brz_01_ingest_customers_raw_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.bronze."
                    "brz_01_ingest_customers_raw_pyarrow:run"
                )
            }
        ),
    ),
    TrackingUnit(
        id="BRZ_02",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-bronze-ingest",
        parent_requirement="Ingest accounts.csv to Bronze",
        family="bronze_ingest",
        variant="accounts_csv_raw",
        candidate_engines=("pyspark_delta", "polars", "pyarrow_acero"),
        validation_gate="Bronze accounts Delta table created and readable",
        description="Raw accounts CSV ingest with explicit schema application.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "pyspark_delta": (
                    "benchmarks.workloads.bronze."
                    "brz_02_ingest_accounts_raw:run"
                ),
                "polars": (
                    "benchmarks.workloads.bronze."
                    "brz_02_ingest_accounts_raw_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.bronze."
                    "brz_02_ingest_accounts_raw_pyarrow:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="BRZ_03",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-bronze-ingest",
        parent_requirement="Ingest transactions.jsonl to Bronze",
        family="bronze_ingest",
        variant="transactions_jsonl_raw",
        candidate_engines=("pyspark_delta", "polars", "pyarrow_acero"),
        validation_gate="Bronze transactions Delta table created and readable",
        description="Raw transaction JSONL ingest with nested schema application.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "pyspark_delta": (
                    "benchmarks.workloads.bronze."
                    "brz_03_ingest_transactions_raw:run"
                ),
                "polars": (
                    "benchmarks.workloads.bronze."
                    "brz_03_ingest_transactions_raw_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.bronze."
                    "brz_03_ingest_transactions_raw_pyarrow:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="BRZ_04",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-bronze-ingest",
        parent_requirement="Add one consistent ingestion_timestamp per ingestion run",
        family="bronze_ingest",
        variant="run_level_ingestion_timestamp",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="All Bronze rows share the same run-level ingestion timestamp",
        description="Assign a single ingestion timestamp for the ingestion run.",
    ),
    TrackingUnit(
        id="BRZ_05",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-bronze-ingest",
        parent_requirement="Write Bronze as Delta format",
        family="delta_write",
        variant="bronze_layer",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Bronze Delta metadata exists and tables round-trip",
        description="Write Bronze outputs as Delta tables.",
    ),
    TrackingUnit(
        id="SLV_01",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Standardise customers types and date formats",
        family="silver_standardisation",
        variant="customers_cast_dates",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="Customers Silver output has expected types",
        description="Type-cast and date-normalise customers data.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.silver."
                    "slv_01_customers_standardise_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.silver."
                    "slv_01_customers_standardise_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.silver."
                    "slv_01_customers_standardise_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="SLV_02",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Standardise accounts types and date formats",
        family="silver_standardisation",
        variant="accounts_cast_dates",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="Accounts Silver output has expected types",
        description="Type-cast and date-normalise accounts data.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.silver."
                    "slv_02_accounts_standardise_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.silver."
                    "slv_02_accounts_standardise_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.silver."
                    "slv_02_accounts_standardise_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="SLV_03",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Standardise transactions types and date formats",
        family="silver_standardisation",
        variant="transactions_parse_standardise",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="Transactions Silver output has expected types and timestamp fields",
        description="Parse transactions, standardise currency, amount, and timestamps.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.silver."
                    "slv_03_transactions_standardise_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.silver."
                    "slv_03_transactions_standardise_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.silver."
                    "slv_03_transactions_standardise_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="SLV_04",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Deduplicate customers on primary key",
        family="silver_dedup",
        variant="customers_pk",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="Customers Silver output contains unique customer_id values",
        description="Deduplicate customers on natural key.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.silver."
                    "slv_04_customers_dedup_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.silver."
                    "slv_04_customers_dedup_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.silver."
                    "slv_04_customers_dedup_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="SLV_05",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Deduplicate accounts on primary key",
        family="silver_dedup",
        variant="accounts_pk",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="Accounts Silver output contains unique account_id values",
        description="Deduplicate accounts on natural key.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.silver."
                    "slv_05_accounts_dedup_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.silver."
                    "slv_05_accounts_dedup_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.silver."
                    "slv_05_accounts_dedup_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="SLV_06",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Deduplicate transactions on primary key",
        family="silver_dedup",
        variant="transactions_pk",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="Transactions Silver output contains unique transaction_id values",
        description="Deduplicate transactions on natural key.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.silver."
                    "slv_06_transactions_dedup_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.silver."
                    "slv_06_transactions_dedup_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.silver."
                    "slv_06_transactions_dedup_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="SLV_07",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Enforce standard currency representation",
        family="silver_standardisation",
        variant="currency_standardise",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="Transactions Silver output uses canonical currency representation",
        description="Standardise currency values to ZAR.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.silver."
                    "slv_07_transactions_currency_standardise_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.silver."
                    "slv_07_transactions_currency_standardise_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.silver."
                    "slv_07_transactions_currency_standardise_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="SLV_08",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Resolve account-to-customer linkage",
        family="silver_linkage",
        variant="account_customer_linkage",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="Accounts link cleanly to customers on the Silver path",
        description="Resolve account-to-customer linkage before Gold joins.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.silver."
                    "slv_08_account_customer_linkage_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.silver."
                    "slv_08_account_customer_linkage_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.silver."
                    "slv_08_account_customer_linkage_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="GLD_01",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-gold-dimensional-model",
        parent_requirement="Build dim_customers",
        family="gold_dimension_build",
        variant="dim_customers",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="dim_customers matches required Gold schema",
        description="Build customer dimension output.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.gold."
                    "gld_01_dim_customers_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.gold."
                    "gld_01_dim_customers_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.gold."
                    "gld_01_dim_customers_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="GLD_02",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-gold-dimensional-model",
        parent_requirement="Build dim_accounts",
        family="gold_dimension_build",
        variant="dim_accounts",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="dim_accounts matches required Gold schema",
        description="Build account dimension output.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.gold."
                    "gld_02_dim_accounts_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.gold."
                    "gld_02_dim_accounts_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.gold."
                    "gld_02_dim_accounts_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="GLD_03",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-gold-dimensional-model",
        parent_requirement="Build fact_transactions",
        family="gold_fact_build",
        variant="fact_transactions",
        candidate_engines=("polars", "pyarrow_acero", "pyspark_delta"),
        validation_gate="fact_transactions matches required Gold schema",
        description="Build transaction fact output.",
        implementations=ImplementationBundle(
            engine_python_refs={
                "polars": (
                    "benchmarks.workloads.gold."
                    "gld_03_fact_transactions_polars:run"
                ),
                "pyarrow_acero": (
                    "benchmarks.workloads.gold."
                    "gld_03_fact_transactions_pyarrow:run"
                ),
                "pyspark_delta": (
                    "benchmarks.workloads.gold."
                    "gld_03_fact_transactions_pyspark:run"
                ),
            }
        ),
    ),
    TrackingUnit(
        id="GLD_04",
        stage="stage1",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage1-gold-dimensional-model",
        parent_requirement="Generate stable surrogate keys",
        family="gold_key_generation",
        variant="stable_surrogate_keys",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Gold surrogate keys are non-null and stable",
        description="Generate stable surrogate keys for Gold tables.",
    ),
    TrackingUnit(
        id="CTR_01",
        stage="stage1",
        role=TrackingRole.CONTRACT,
        branch_owner="feature/stage1-bronze-ingest",
        parent_requirement="Bronze layer contract",
        family="contract",
        variant="bronze_layer",
        candidate_engines=(),
        validation_gate="Bronze outputs satisfy authoritative path and Delta requirements",
        description="Bronze layer contract for paths, timestamps, and Delta output.",
    ),
    TrackingUnit(
        id="CTR_02",
        stage="stage1",
        role=TrackingRole.CONTRACT,
        branch_owner="feature/stage1-silver-standardisation",
        parent_requirement="Silver layer contract",
        family="contract",
        variant="silver_layer",
        candidate_engines=(),
        validation_gate="Silver outputs are typed, deduplicated, and join-ready",
        description="Silver layer contract for typed, deduplicated outputs.",
    ),
    TrackingUnit(
        id="CTR_03",
        stage="stage1",
        role=TrackingRole.CONTRACT,
        branch_owner="feature/stage1-gold-dimensional-model",
        parent_requirement="Gold layer contract",
        family="contract",
        variant="gold_layer",
        candidate_engines=(),
        validation_gate="Gold outputs satisfy schema, FK, and key invariants",
        description="Gold layer contract for dimensional outputs.",
    ),
    TrackingUnit(
        id="VAL_01",
        stage="stage1",
        role=TrackingRole.GATE,
        branch_owner="feature/stage1-validation-queries",
        parent_requirement="Pass validation query: transaction volume by type",
        family="validation_query",
        variant="txn_volume_by_type",
        candidate_engines=(),
        validation_gate="Validation query returns 4 transaction-type groups",
        description="Acceptance gate for validation query 1.",
    ),
    TrackingUnit(
        id="VAL_02",
        stage="stage1",
        role=TrackingRole.GATE,
        branch_owner="feature/stage1-validation-queries",
        parent_requirement="Pass validation query: zero unlinked accounts",
        family="validation_query",
        variant="zero_unlinked_accounts",
        candidate_engines=(),
        validation_gate="Validation query returns zero unlinked accounts",
        description="Acceptance gate for validation query 2.",
    ),
    TrackingUnit(
        id="VAL_03",
        stage="stage1",
        role=TrackingRole.GATE,
        branch_owner="feature/stage1-validation-queries",
        parent_requirement="Pass validation query: province distribution",
        family="validation_query",
        variant="province_distribution",
        candidate_engines=(),
        validation_gate="Validation query returns complete province distribution",
        description="Acceptance gate for validation query 3.",
    ),
    TrackingUnit(
        id="S2_SE_01",
        stage="stage2",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage2-schema-evolution",
        parent_requirement="Accept new merchant_subcategory field in transactions",
        family="schema_evolution",
        variant="merchant_subcategory_optional",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Stage 1 and Stage 2 transaction schemas both parse cleanly",
        description="Handle optional merchant_subcategory across stage boundaries.",
    ),
    TrackingUnit(
        id="S2_DQ_01",
        stage="stage2",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage2-dq-framework",
        parent_requirement="Handle duplicate records via dedup logic",
        family="dq_handling",
        variant="duplicate_deduped",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Duplicates are detected and handled per config",
        description="Handle duplicate transaction delivery.",
    ),
    TrackingUnit(
        id="S2_DQ_02",
        stage="stage2",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage2-dq-framework",
        parent_requirement="Detect and handle orphaned account references",
        family="dq_handling",
        variant="orphaned_account",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Orphaned transactions are handled per config",
        description="Handle transactions without a valid account link.",
    ),
    TrackingUnit(
        id="S2_DQ_03",
        stage="stage2",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage2-dq-framework",
        parent_requirement="Detect and handle type mismatch issues",
        family="dq_handling",
        variant="type_mismatch_amount",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Type mismatches are detected and normalized or flagged",
        description="Handle transaction amount type mismatches.",
    ),
    TrackingUnit(
        id="S2_DQ_04",
        stage="stage2",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage2-dq-framework",
        parent_requirement="Detect and handle inconsistent date formats",
        family="dq_handling",
        variant="date_format_normalise",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Mixed date formats are normalized or flagged",
        description="Handle mixed date formats.",
    ),
    TrackingUnit(
        id="S2_DQ_05",
        stage="stage2",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage2-dq-framework",
        parent_requirement="Detect and handle currency variants",
        family="dq_handling",
        variant="currency_variant_normalise",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Currency variants are normalized to ZAR",
        description="Handle non-standard currency variants.",
    ),
    TrackingUnit(
        id="S2_DQ_06",
        stage="stage2",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage2-dq-framework",
        parent_requirement="Detect and handle null required fields",
        family="dq_handling",
        variant="null_required_account_id",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Null-required records are handled per config",
        description="Handle null required account identifiers.",
    ),
    TrackingUnit(
        id="S2_DQ_07",
        stage="stage2",
        role=TrackingRole.CONTRACT,
        branch_owner="feature/stage2-dq-framework",
        parent_requirement="Externalise DQ handling rules in config",
        family="contract",
        variant="dq_rules_externalised",
        candidate_engines=(),
        validation_gate="DQ rules live in config, not hardcoded logic",
        description="Contract for config-driven DQ handling.",
    ),
    TrackingUnit(
        id="S2_DQ_08",
        stage="stage2",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage2-dq-framework",
        parent_requirement="Produce dq_report.json output",
        family="dq_reporting",
        variant="dq_report_build",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="DQ report serializes and reconciles with flagged records",
        description="Build the Stage 2 DQ report artifact.",
    ),
    TrackingUnit(
        id="S2_VAL_01",
        stage="stage2",
        role=TrackingRole.GATE,
        branch_owner="feature/stage2-scale-hardening",
        parent_requirement="Still pass validation queries under Stage 2 conditions",
        family="validation_query",
        variant="stage2_validation_queries",
        candidate_engines=(),
        validation_gate="Stage 2 outputs still pass the published validation queries",
        description="Stage 2 acceptance gate for published validation queries.",
    ),
    TrackingUnit(
        id="S2_SC_01",
        stage="stage2",
        role=TrackingRole.SCENARIO,
        branch_owner="feature/stage2-scale-hardening",
        parent_requirement="Handle 3x data volume within constraints",
        family="scenario",
        variant="volume_3x_smoke",
        candidate_engines=(),
        validation_gate="Pipeline stays within Stage 2 time and memory limits",
        description="Stage 2 scale scenario for 3x volume.",
    ),
    TrackingUnit(
        id="S3_ST_01",
        stage="stage3",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage3-stream-ingest",
        parent_requirement="Poll stream directory and process files in order",
        family="stream_ingest",
        variant="stream_discovery_order",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Stream files are processed exactly once in filename order",
        description="Poll and discover stream files in order.",
    ),
    TrackingUnit(
        id="S3_ST_02",
        stage="stage3",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage3-stream-ingest",
        parent_requirement="Parse and standardise stream transactions",
        family="stream_ingest",
        variant="stream_parse_normalise",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="Stream events parse and normalize against the batch contract",
        description="Parse and normalize stream transactions.",
    ),
    TrackingUnit(
        id="S3_ST_03",
        stage="stage3",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage3-stream-state-tables",
        parent_requirement="Maintain current_balances table",
        family="stream_state",
        variant="current_balances_merge",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="current_balances updates by account_id merge semantics",
        description="Maintain the current_balances state table.",
    ),
    TrackingUnit(
        id="S3_ST_04",
        stage="stage3",
        role=TrackingRole.WORKLOAD,
        branch_owner="feature/stage3-stream-state-tables",
        parent_requirement="Maintain recent_transactions table",
        family="stream_state",
        variant="recent_transactions_top50",
        candidate_engines=SHORTLIST_ENGINES,
        validation_gate="recent_transactions retains top 50 rows per account",
        description="Maintain recent_transactions with retention behavior.",
    ),
    TrackingUnit(
        id="S3_ST_05",
        stage="stage3",
        role=TrackingRole.SCENARIO,
        branch_owner="feature/stage3-stream-state-tables",
        parent_requirement="Enforce updated_at SLA",
        family="scenario",
        variant="stream_updated_at_sla",
        candidate_engines=(),
        validation_gate="updated_at stays within the Stage 3 SLA window",
        description="Stage 3 SLA scenario for stream outputs.",
    ),
    TrackingUnit(
        id="S3_ST_06",
        stage="stage3",
        role=TrackingRole.SCENARIO,
        branch_owner="feature/stage3-stream-ingest",
        parent_requirement="Keep batch and stream paths working together",
        family="scenario",
        variant="stream_batch_coexistence",
        candidate_engines=(),
        validation_gate="Batch outputs remain valid while stream processing runs",
        description="Stage 3 coexistence scenario for batch and stream paths.",
    ),
    TrackingUnit(
        id="S3_ST_07",
        stage="stage3",
        role=TrackingRole.SCENARIO,
        branch_owner="feature/stage3-stream-ingest",
        parent_requirement="Avoid reprocessing already handled stream files",
        family="scenario",
        variant="stream_replay_safety",
        candidate_engines=(),
        validation_gate="Processed stream files are not replayed",
        description="Stage 3 replay-safety scenario.",
    ),
    TrackingUnit(
        id="S3_ADR_01",
        stage="stage3",
        role=TrackingRole.GATE,
        branch_owner="docs/stage3-adr",
        parent_requirement="Submit adr/stage3_adr.md",
        family="documentation_gate",
        variant="stage3_adr_submission",
        candidate_engines=(),
        validation_gate="ADR exists and covers the three required prompts",
        description="Stage 3 ADR submission gate.",
    ),
)


def filter_manifest(
    *,
    role: TrackingRole | None = None,
    stage: str | None = None,
    branch_owner: str | None = None,
) -> list[TrackingUnit]:
    """Return manifest entries filtered by role, stage, and/or branch owner."""

    items = list(TRACKING_MANIFEST)
    if role is not None:
        items = [item for item in items if item.role == role]
    if stage is not None:
        items = [item for item in items if item.stage == stage]
    if branch_owner is not None:
        items = [item for item in items if item.branch_owner == branch_owner]
    return items


def manifest_as_dicts(
    *,
    role: TrackingRole | None = None,
    stage: str | None = None,
    branch_owner: str | None = None,
) -> list[dict[str, object]]:
    """Return serialized manifest entries suitable for CLI output."""

    return [
        item.as_dict()
        for item in filter_manifest(role=role, stage=stage, branch_owner=branch_owner)
    ]
