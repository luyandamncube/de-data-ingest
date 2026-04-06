"""Polars benchmark workload for BRZ_01 customers Bronze ingestion."""

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


def _polars_schema() -> dict[str, object]:
    import polars as pl

    return {field.name: pl.String for field in BRONZE_CUSTOMERS_SCHEMA.fields}


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Read customers.csv, add a run-level timestamp, and write Bronze Delta."""

    from deltalake import DeltaTable
    import polars as pl

    customers_path = data_root / "customers.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    rows_in = _count_csv_rows(customers_path)
    output_path = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    shutil.rmtree(output_path, ignore_errors=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    customers_df = pl.read_csv(
        customers_path,
        schema=_polars_schema(),
    )
    customers_with_ts = customers_df.with_columns(
        pl.lit(run_timestamp).cast(pl.Datetime("us")).alias("ingestion_timestamp")
    )

    write_start = time.perf_counter()
    customers_with_ts.write_delta(str(output_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    delta_table = DeltaTable(str(output_path))
    if hasattr(delta_table, "file_uris"):
        active_files = delta_table.file_uris()
    else:
        active_files = [
            str(output_path / relative_path) for relative_path in delta_table.files()
        ]

    validation_row = (
        pl.scan_parquet(active_files)
        .select(
            pl.len().alias("row_count"),
            pl.col("ingestion_timestamp").min().alias("min_ingestion_timestamp"),
            pl.col("ingestion_timestamp").max().alias("max_ingestion_timestamp"),
        )
        .collect()
        .row(0, named=True)
    )
    rows_out = validation_row["row_count"]
    min_ingestion_timestamp = validation_row["min_ingestion_timestamp"]
    max_ingestion_timestamp = validation_row["max_ingestion_timestamp"]
    delta_read_seconds = time.perf_counter() - read_start

    validation_seconds = delta_read_seconds
    cpu_pct = (
        (cpu_user_seconds / core_elapsed_seconds) * 100.0
        if core_elapsed_seconds > 0
        else None
    )
    output_file_count = len(list(output_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_in == rows_out and min_ingestion_timestamp == max_ingestion_timestamp
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
            "Polars Bronze customers ingest benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "output_path": str(output_path),
            "min_ingestion_timestamp": str(min_ingestion_timestamp),
            "max_ingestion_timestamp": str(max_ingestion_timestamp),
            "implementation_ref": (
                "benchmarks.workloads.bronze."
                "brz_01_ingest_customers_raw_polars:run"
            ),
        },
    )
