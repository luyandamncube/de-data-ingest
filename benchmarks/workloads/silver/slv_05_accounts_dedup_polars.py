"""Polars benchmark workload for SLV_05 accounts Silver deduplication."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.silver.slv_05_accounts_dedup_common import (
    DEDUP_FRACTION,
    active_file_uris,
    build_duplicate_accounts_fixture,
)
from pipeline.registry import TrackingUnit


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


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Read duplicated Silver-like accounts Delta, deduplicate, and write Silver."""

    from deltalake import DeltaTable
    import polars as pl

    accounts_path = data_root / "accounts.csv"
    if not accounts_path.exists():
        raise FileNotFoundError(f"accounts dataset not found: {accounts_path}")

    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    bronze_path = attempt_root / "silver_like_accounts_with_dups"
    deduped_path = attempt_root / "silver_accounts_deduped"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    base_rows, duplicate_rows, rows_in = build_duplicate_accounts_fixture(
        accounts_path=accounts_path,
        output_path=bronze_path,
        run_timestamp=run_timestamp,
    )

    fixture_table = DeltaTable(str(bronze_path))
    fixture_files = active_file_uris(fixture_table, bronze_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    deduped_df = (
        pl.scan_parquet(fixture_files)
        .sort(["account_id", "ingestion_timestamp"], descending=[False, True])
        .unique(subset=["account_id"], keep="first", maintain_order=True)
        .collect()
    )

    write_start = time.perf_counter()
    deduped_df.write_delta(str(deduped_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    deduped_table = DeltaTable(str(deduped_path))
    deduped_files = active_file_uris(deduped_table, deduped_path)
    validation_row = (
        pl.scan_parquet(deduped_files)
        .select(
            pl.len().alias("row_count"),
            pl.col("account_id").n_unique().alias("unique_account_ids"),
            pl.col("account_id").null_count().alias("null_account_id_count"),
            pl.col("customer_ref").null_count().alias("null_customer_ref_count"),
            pl.col("open_date").null_count().alias("null_open_date_count"),
            pl.col("current_balance").null_count().alias("null_current_balance_count"),
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
        and validation_row["unique_account_ids"] == base_rows
        and validation_row["null_account_id_count"] == 0
        and validation_row["null_customer_ref_count"] == 0
        and validation_row["null_open_date_count"] == 0
        and validation_row["null_current_balance_count"] == 0
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
            "Polars Silver accounts dedup benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
            f"; duplicate_fraction={DEDUP_FRACTION:.2f}"
        ),
        artifacts={
            "input_path": str(accounts_path),
            "fixture_path": str(bronze_path),
            "output_path": str(deduped_path),
            "base_rows": base_rows,
            "duplicate_rows": duplicate_rows,
            "unique_account_ids": validation_row["unique_account_ids"],
            "null_account_id_count": validation_row["null_account_id_count"],
            "null_customer_ref_count": validation_row["null_customer_ref_count"],
            "null_open_date_count": validation_row["null_open_date_count"],
            "null_current_balance_count": validation_row["null_current_balance_count"],
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_05_accounts_dedup_polars:run"
            ),
        },
    )