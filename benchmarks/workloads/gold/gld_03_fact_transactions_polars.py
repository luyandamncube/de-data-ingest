"""Polars benchmark workload for GLD_03 fact_transactions Gold build."""

from __future__ import annotations

from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.gold.gld_03_fact_transactions_common import (
    active_file_uris,
    build_fact_transactions_fixtures,
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
    """Build a Gold fact_transactions output from source fixtures."""

    from deltalake import DeltaTable
    import polars as pl

    accounts_path = data_root / "accounts.csv"
    customers_path = data_root / "customers.csv"
    transactions_path = data_root / "transactions.jsonl"

    if not accounts_path.exists():
        raise FileNotFoundError(f"accounts dataset not found: {accounts_path}")
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")
    if not transactions_path.exists():
        raise FileNotFoundError(f"transactions dataset not found: {transactions_path}")

    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    accounts_fixture_path = attempt_root / "silver_like_accounts"
    customers_fixture_path = attempt_root / "silver_like_customers"
    transactions_fixture_path = attempt_root / "silver_like_transactions"
    fact_path = attempt_root / "gold_fact_transactions"

    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    account_rows, customer_rows, transaction_rows, rows_in = build_fact_transactions_fixtures(
        accounts_path=accounts_path,
        customers_path=customers_path,
        transactions_path=transactions_path,
        accounts_output_path=accounts_fixture_path,
        customers_output_path=customers_fixture_path,
        transactions_output_path=transactions_fixture_path,
    )

    accounts_table = DeltaTable(str(accounts_fixture_path))
    customers_table = DeltaTable(str(customers_fixture_path))
    transactions_table = DeltaTable(str(transactions_fixture_path))

    account_files = active_file_uris(accounts_table, accounts_fixture_path)
    customer_files = active_file_uris(customers_table, customers_fixture_path)
    transaction_files = active_file_uris(transactions_table, transactions_fixture_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    account_map_df = (
        pl.scan_parquet(account_files)
        .select(
            "account_id",
            pl.col("customer_ref").alias("linked_customer_id"),
        )
        .collect()
        .sort("account_id")
        .with_row_index("account_sk", offset=1)
        .with_columns(pl.col("account_sk").cast(pl.Int64))
    )

    customer_map_df = (
        pl.scan_parquet(customer_files)
        .select("customer_id")
        .collect()
        .sort("customer_id")
        .with_row_index("customer_sk", offset=1)
        .with_columns(pl.col("customer_sk").cast(pl.Int64))
    )

    account_map_lf = account_map_df.lazy()
    customer_map_lf = customer_map_df.lazy()

    fact_df = (
        pl.scan_parquet(transaction_files)
        .with_columns(
            pl.concat_str(
                [
                    pl.col("transaction_date").cast(pl.Utf8),
                    pl.col("transaction_time").cast(pl.Utf8),
                ],
                separator=" ",
            )
            .str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=True)
            .alias("transaction_timestamp"),
            pl.col("currency").cast(pl.Utf8).str.to_uppercase().alias("currency"),
        )
        .join(account_map_lf, on="account_id", how="left")
        .join(
            customer_map_lf,
            left_on="linked_customer_id",
            right_on="customer_id",
            how="left",
        )
        .collect()
        .sort("transaction_id")
        .with_row_index("transaction_sk", offset=1)
        .with_columns(
            pl.col("transaction_sk").cast(pl.Int64),
            pl.lit(None, dtype=pl.Utf8).alias("dq_flag"),
        )
        .select(
            "transaction_sk",
            "transaction_id",
            "account_sk",
            "customer_sk",
            "transaction_timestamp",
            "transaction_date",
            "transaction_type",
            "merchant_category",
            "merchant_subcategory",
            "amount",
            "currency",
            "channel",
            "location_province",
            "location_city",
            "location_coordinates",
            "metadata_device_id",
            "metadata_session_id",
            "metadata_retry_flag",
            "dq_flag",
        )
    )

    write_start = time.perf_counter()
    fact_df.write_delta(str(fact_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    fact_table = DeltaTable(str(fact_path))
    fact_files = active_file_uris(fact_table, fact_path)

    validation_df = pl.scan_parquet(fact_files).collect()
    validation_row = (
        validation_df.select(
            pl.len().alias("row_count"),
            pl.col("transaction_sk").n_unique().alias("unique_transaction_sks"),
            pl.col("transaction_id").n_unique().alias("unique_transaction_ids"),
            pl.col("transaction_sk").null_count().alias("null_transaction_sk_count"),
            pl.col("account_sk").null_count().alias("null_account_sk_count"),
            pl.col("customer_sk").null_count().alias("null_customer_sk_count"),
            pl.col("transaction_timestamp").null_count().alias("null_transaction_timestamp_count"),
            pl.col("currency").null_count().alias("null_currency_count"),
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
    output_file_count = len(list(fact_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_out == transaction_rows
        and validation_row["unique_transaction_sks"] == transaction_rows
        and validation_row["unique_transaction_ids"] == transaction_rows
        and validation_row["null_transaction_sk_count"] == 0
        and validation_row["null_account_sk_count"] == 0
        and validation_row["null_customer_sk_count"] == 0
        and validation_row["null_transaction_timestamp_count"] == 0
        and validation_row["null_currency_count"] == 0
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
            "Polars Gold fact_transactions benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "accounts_input_path": str(accounts_path),
            "customers_input_path": str(customers_path),
            "transactions_input_path": str(transactions_path),
            "accounts_fixture_path": str(accounts_fixture_path),
            "customers_fixture_path": str(customers_fixture_path),
            "transactions_fixture_path": str(transactions_fixture_path),
            "output_path": str(fact_path),
            "account_rows": account_rows,
            "customer_rows": customer_rows,
            "transaction_rows": transaction_rows,
            "output_columns": validation_df.columns,
            "unique_transaction_sks": validation_row["unique_transaction_sks"],
            "unique_transaction_ids": validation_row["unique_transaction_ids"],
            "null_transaction_sk_count": validation_row["null_transaction_sk_count"],
            "null_account_sk_count": validation_row["null_account_sk_count"],
            "null_customer_sk_count": validation_row["null_customer_sk_count"],
            "null_transaction_timestamp_count": validation_row["null_transaction_timestamp_count"],
            "null_currency_count": validation_row["null_currency_count"],
            "implementation_ref": (
                "benchmarks.workloads.gold."
                "gld_03_fact_transactions_polars:run"
            ),
        },
    )