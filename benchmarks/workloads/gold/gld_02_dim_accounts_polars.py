"""Polars benchmark workload for GLD_02 dim_accounts Gold build."""

from __future__ import annotations

from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.gold.gld_02_dim_accounts_common import (
    active_file_uris,
    build_dim_accounts_fixture,
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
    """Build a Gold dim_accounts output from the accounts source."""

    from deltalake import DeltaTable
    import polars as pl

    accounts_path = data_root / "accounts.csv"
    if not accounts_path.exists():
        raise FileNotFoundError(f"accounts dataset not found: {accounts_path}")

    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    fixture_path = attempt_root / "silver_like_accounts"
    dim_path = attempt_root / "gold_dim_accounts"

    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    base_rows, rows_in = build_dim_accounts_fixture(
        accounts_path=accounts_path,
        output_path=fixture_path,
    )

    fixture_table = DeltaTable(str(fixture_path))
    fixture_files = active_file_uris(fixture_table, fixture_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    dim_df = (
        pl.scan_parquet(fixture_files)
        .with_columns(
            pl.col("open_date").str.strptime(pl.Date, format="%Y-%m-%d", strict=True),
            pl.col("last_activity_date").str.strptime(
                pl.Date, format="%Y-%m-%d", strict=False
            ),
            pl.col("customer_ref").alias("customer_id"),
        )
        .drop("customer_ref")
        .collect()
        .sort("account_id")
        .with_row_index("account_sk", offset=1)
        .with_columns(pl.col("account_sk").cast(pl.Int64))
        .select(
            "account_sk",
            "account_id",
            "customer_id",
            "account_type",
            "account_status",
            "open_date",
            "product_tier",
            "mobile_number",
            "digital_channel",
            "credit_limit",
            "current_balance",
            "last_activity_date",
        )
    )

    write_start = time.perf_counter()
    dim_df.write_delta(str(dim_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    dim_table = DeltaTable(str(dim_path))
    dim_files = active_file_uris(dim_table, dim_path)

    validation_df = pl.scan_parquet(dim_files).collect()
    validation_row = (
        validation_df.select(
            pl.len().alias("row_count"),
            pl.col("account_sk").n_unique().alias("unique_account_sks"),
            pl.col("account_id").n_unique().alias("unique_account_ids"),
            pl.col("account_sk").null_count().alias("null_account_sk_count"),
            pl.col("account_id").null_count().alias("null_account_id_count"),
            pl.col("customer_id").null_count().alias("null_customer_id_count"),
            pl.col("open_date").null_count().alias("null_open_date_count"),
            pl.col("current_balance").null_count().alias("null_current_balance_count"),
        )
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
    output_file_count = len(list(dim_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_out == base_rows
        and validation_row["unique_account_sks"] == base_rows
        and validation_row["unique_account_ids"] == base_rows
        and validation_row["null_account_sk_count"] == 0
        and validation_row["null_account_id_count"] == 0
        and validation_row["null_customer_id_count"] == 0
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
            "Polars Gold dim_accounts benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(accounts_path),
            "fixture_path": str(fixture_path),
            "output_path": str(dim_path),
            "base_rows": base_rows,
            "output_columns": validation_df.columns,
            "unique_account_sks": validation_row["unique_account_sks"],
            "unique_account_ids": validation_row["unique_account_ids"],
            "null_account_sk_count": validation_row["null_account_sk_count"],
            "null_account_id_count": validation_row["null_account_id_count"],
            "null_customer_id_count": validation_row["null_customer_id_count"],
            "null_open_date_count": validation_row["null_open_date_count"],
            "null_current_balance_count": validation_row["null_current_balance_count"],
            "implementation_ref": (
                "benchmarks.workloads.gold."
                "gld_02_dim_accounts_polars:run"
            ),
        },
    )