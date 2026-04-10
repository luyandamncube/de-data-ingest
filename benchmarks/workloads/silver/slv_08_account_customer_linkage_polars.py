"""Polars benchmark workload for SLV_08 account-customer Silver linkage."""

from __future__ import annotations

from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.silver.slv_08_account_customer_linkage_common import (
    active_file_uris,
    build_account_customer_linkage_fixture,
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
    """Resolve account-to-customer linkage and write a Silver join-ready output."""

    from deltalake import DeltaTable
    import polars as pl

    accounts_path = data_root / "accounts.csv"
    customers_path = data_root / "customers.csv"

    if not accounts_path.exists():
        raise FileNotFoundError(f"accounts dataset not found: {accounts_path}")
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    accounts_fixture_path = attempt_root / "silver_like_accounts"
    customers_fixture_path = attempt_root / "silver_like_customers"
    linked_output_path = attempt_root / "silver_accounts_linked"

    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    account_rows, customer_rows, rows_in = build_account_customer_linkage_fixture(
        accounts_path=accounts_path,
        customers_path=customers_path,
        accounts_output_path=accounts_fixture_path,
        customers_output_path=customers_fixture_path,
    )

    accounts_table = DeltaTable(str(accounts_fixture_path))
    customers_table = DeltaTable(str(customers_fixture_path))

    account_files = active_file_uris(accounts_table, accounts_fixture_path)
    customer_files = active_file_uris(customers_table, customers_fixture_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    accounts_lf = pl.scan_parquet(account_files)

    customers_lf = (
        pl.scan_parquet(customer_files)
        .select(
            pl.col("customer_id").alias("linked_customer_id"),
            pl.lit(True).alias("customer_match"),
        )
    )

    linked_df = (
        accounts_lf
        .join(
            customers_lf,
            left_on="customer_ref",
            right_on="linked_customer_id",
            how="left",
        )
        .with_columns(
            pl.col("customer_match").fill_null(False).alias("link_resolved"),
            pl.col("customer_ref").alias("customer_id"),
        )
        .drop("customer_ref", "customer_match")
        .collect()
    )
    write_start = time.perf_counter()
    linked_df.write_delta(str(linked_output_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    linked_table = DeltaTable(str(linked_output_path))
    linked_files = active_file_uris(linked_table, linked_output_path)

    validation_row = (
        pl.scan_parquet(linked_files)
        .select(
            pl.len().alias("row_count"),
            pl.col("account_id").n_unique().alias("unique_account_ids"),
            pl.col("account_id").null_count().alias("null_account_id_count"),
            pl.col("customer_id").null_count().alias("null_customer_id_count"),
            pl.col("link_resolved").cast(pl.Int64).sum().alias("resolved_link_count"),
            pl.col("link_resolved")
            .filter(pl.col("link_resolved").not_())
            .len()
            .alias("unmatched_account_count"),
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
    output_file_count = len(list(linked_output_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_out == account_rows
        and validation_row["unique_account_ids"] == account_rows
        and validation_row["null_account_id_count"] == 0
        and validation_row["null_customer_id_count"] == 0
        and validation_row["resolved_link_count"] == account_rows
        and validation_row["unmatched_account_count"] == 0
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
            "Polars Silver account-customer linkage benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "accounts_input_path": str(accounts_path),
            "customers_input_path": str(customers_path),
            "accounts_fixture_path": str(accounts_fixture_path),
            "customers_fixture_path": str(customers_fixture_path),
            "output_path": str(linked_output_path),
            "account_rows": account_rows,
            "customer_rows": customer_rows,
            "resolved_link_count": validation_row["resolved_link_count"],
            "unmatched_account_count": validation_row["unmatched_account_count"],
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_08_account_customer_linkage_polars:run"
            ),
        },
    )