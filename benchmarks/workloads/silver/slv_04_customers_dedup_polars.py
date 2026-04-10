"""Polars benchmark workload for SLV_04 customers Silver deduplication."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from pipeline.registry import TrackingUnit
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA


DEDUP_FRACTION = 0.10


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
    """Read duplicated Silver-like customers Delta, deduplicate, and write Silver."""

    from deltalake import DeltaTable
    import polars as pl

    customers_path = data_root / "customers.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    base_rows = _count_csv_rows(customers_path)
    duplicate_rows = int(base_rows * DEDUP_FRACTION)
    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    bronze_path = attempt_root / "silver_like_customers_with_dups"
    deduped_path = attempt_root / "silver_customers_deduped"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    base_df = (
        pl.read_csv(
            customers_path,
            schema=_bronze_polars_schema(),
        )
        .with_row_index("source_row_nr")
        .with_columns(
            [
                pl.col("dob").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                pl.col("risk_score").cast(pl.Int32, strict=False),
                pl.lit(run_timestamp).cast(pl.Datetime("us")).alias(
                    "ingestion_timestamp"
                ),
            ]
        )
    )
    duplicate_df = (
        base_df.head(duplicate_rows)
        .with_columns(
            [
                pl.col("source_row_nr"),
                (pl.col("ingestion_timestamp") + timedelta(seconds=1)).alias(
                    "ingestion_timestamp"
                ),
            ]
        )
    )
    duplicated_df = pl.concat([base_df, duplicate_df], how="vertical")
    fixture_df = duplicated_df.drop("source_row_nr")
    fixture_df.write_delta(str(bronze_path), mode="overwrite")

    fixture_table = DeltaTable(str(bronze_path))
    fixture_files = _active_file_uris(fixture_table, bronze_path)
    rows_in = fixture_df.height

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    deduped_df = (
        pl.scan_parquet(fixture_files)
        .sort(["customer_id", "ingestion_timestamp"], descending=[False, True])
        .unique(subset=["customer_id"], keep="first", maintain_order=True)
        .collect()
    )

    write_start = time.perf_counter()
    deduped_df.write_delta(str(deduped_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    deduped_table = DeltaTable(str(deduped_path))
    deduped_files = _active_file_uris(deduped_table, deduped_path)
    validation_row = (
        pl.scan_parquet(deduped_files)
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
    output_file_count = len(list(deduped_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_out == base_rows
        and validation_row["unique_customer_ids"] == base_rows
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
            "Polars Silver customers dedup benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
            f"; duplicate_fraction={DEDUP_FRACTION:.2f}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "fixture_path": str(bronze_path),
            "output_path": str(deduped_path),
            "base_rows": base_rows,
            "duplicate_rows": duplicate_rows,
            "unique_customer_ids": validation_row["unique_customer_ids"],
            "null_dob_count": validation_row["null_dob_count"],
            "null_risk_score_count": validation_row["null_risk_score_count"],
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_04_customers_dedup_polars:run"
            ),
        },
    )
