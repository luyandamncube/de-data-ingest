"""Polars benchmark workload for SLV_01 customers Silver standardisation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from pipeline.registry import TrackingUnit
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA


def _peak_memory_mb() -> float:
    self_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    children_usage = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return max(self_usage, children_usage) / 1024.0


def _cpu_user_seconds() -> float:
    self_usage = resource.getrusage(resource.RUSAGE_SELF)
    children_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return (
        self_usage.ru_utime
        + self_usage.ru_stime
        + children_usage.ru_utime
        + children_usage.ru_stime
    )


def _count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def _bronze_polars_schema() -> dict[str, object]:
    import polars as pl

    # Use the current Bronze field order, but keep the setup permissive so the
    # benchmark focuses on the Silver standardisation cost.
    return {field.name: pl.String for field in BRONZE_CUSTOMERS_SCHEMA.fields}


def _active_file_uris(delta_table, table_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(table_path / relative_path) for relative_path in delta_table.files()]


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Read Bronze customers Delta, standardise customer types, and write Silver."""

    from deltalake import DeltaTable
    import polars as pl

    customers_path = data_root / "customers.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    rows_in = _count_csv_rows(customers_path)
    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    bronze_path = attempt_root / "bronze_customers"
    silver_path = attempt_root / "silver_customers"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    bronze_df = pl.read_csv(
        customers_path,
        schema=_bronze_polars_schema(),
    ).with_columns(
        pl.lit(run_timestamp).cast(pl.Datetime("us")).alias("ingestion_timestamp")
    )
    bronze_df.write_delta(str(bronze_path), mode="overwrite")

    bronze_table = DeltaTable(str(bronze_path))
    bronze_files = _active_file_uris(bronze_table, bronze_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    silver_df = (
        pl.scan_parquet(bronze_files)
        .select(
            [
                pl.col("customer_id").cast(pl.String),
                pl.col("id_number").cast(pl.String),
                pl.col("first_name").cast(pl.String),
                pl.col("last_name").cast(pl.String),
                pl.col("dob").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                pl.col("gender").cast(pl.String),
                pl.col("province").cast(pl.String),
                pl.col("income_band").cast(pl.String),
                pl.col("segment").cast(pl.String),
                pl.col("risk_score").cast(pl.Int32, strict=False),
                pl.col("kyc_status").cast(pl.String),
                pl.col("product_flags").cast(pl.String),
                pl.col("ingestion_timestamp").cast(pl.Datetime("us")),
            ]
        )
        .collect()
    )

    write_start = time.perf_counter()
    silver_df.write_delta(str(silver_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    silver_table = DeltaTable(str(silver_path))
    silver_files = _active_file_uris(silver_table, silver_path)
    validation_row = (
        pl.scan_parquet(silver_files)
        .select(
            pl.len().alias("row_count"),
            pl.col("customer_id").n_unique().alias("unique_customer_ids"),
            pl.col("dob").null_count().alias("null_dob_count"),
            pl.col("risk_score").null_count().alias("null_risk_score_count"),
        )
        .collect()
        .row(0, named=True)
    )
    rows_out = validation_row["row_count"]
    delta_read_seconds = time.perf_counter() - read_start

    validation_seconds = delta_read_seconds
    cpu_pct = (
        (cpu_user_seconds / core_elapsed_seconds) * 100.0
        if core_elapsed_seconds > 0
        else None
    )
    output_file_count = len(list(silver_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_in == rows_out
        and validation_row["unique_customer_ids"] == rows_out
        and validation_row["null_dob_count"] == 0
        and validation_row["null_risk_score_count"] == 0
    )

    return BenchmarkExecution(
        workload_id=workload.id,
        engine=engine_name,
        elapsed_seconds=core_elapsed_seconds,
        peak_memory_mb=_peak_memory_mb(),
        validation_seconds=validation_seconds,
        rows_in=rows_in,
        rows_out=rows_out,
        cpu_user_seconds=cpu_user_seconds,
        cpu_pct=cpu_pct,
        delta_write_seconds=delta_write_seconds,
        delta_read_seconds=delta_read_seconds,
        tmp_peak_mb=None,
        output_file_count=output_file_count,
        delta_roundtrip_ok=delta_roundtrip_ok,
        notes=(
            "Polars Silver customers standardisation benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "bronze_fixture_path": str(bronze_path),
            "output_path": str(silver_path),
            "unique_customer_ids": validation_row["unique_customer_ids"],
            "null_dob_count": validation_row["null_dob_count"],
            "null_risk_score_count": validation_row["null_risk_score_count"],
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_01_customers_standardise_polars:run"
            ),
        },
    )
