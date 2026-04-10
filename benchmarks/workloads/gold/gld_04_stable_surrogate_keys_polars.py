"""Polars benchmark workload for GLD_04 stable surrogate key generation."""

from __future__ import annotations

import hashlib
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.gold.gld_04_stable_surrogate_keys_common import (
    active_file_uris,
    build_stable_surrogate_key_fixtures,
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


def _stable_int64(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) & 0x7FFFFFFFFFFFFFFF


def _customer_key_expr(pl):
    return (
        pl.concat_str([pl.lit("customer:"), pl.col("customer_id").cast(pl.Utf8)])
        .map_elements(_stable_int64, return_dtype=pl.Int64)
        .alias("surrogate_key")
    )


def _account_key_expr(pl):
    return (
        pl.concat_str([pl.lit("account:"), pl.col("account_id").cast(pl.Utf8)])
        .map_elements(_stable_int64, return_dtype=pl.Int64)
        .alias("surrogate_key")
    )


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Generate deterministic surrogate keys and validate stability across rebuilds."""

    from deltalake import DeltaTable
    import polars as pl

    customers_path = data_root / "customers.csv"
    accounts_path = data_root / "accounts.csv"

    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")
    if not accounts_path.exists():
        raise FileNotFoundError(f"accounts dataset not found: {accounts_path}")

    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    customers_fixture_path = attempt_root / "silver_like_customers"
    accounts_fixture_path = attempt_root / "silver_like_accounts"
    key_registry_path = attempt_root / "gold_surrogate_keys"

    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    customer_rows, account_rows, rows_in = build_stable_surrogate_key_fixtures(
        customers_path=customers_path,
        accounts_path=accounts_path,
        customers_output_path=customers_fixture_path,
        accounts_output_path=accounts_fixture_path,
    )

    customers_table = DeltaTable(str(customers_fixture_path))
    accounts_table = DeltaTable(str(accounts_fixture_path))

    customer_files = active_file_uris(customers_table, customers_fixture_path)
    account_files = active_file_uris(accounts_table, accounts_fixture_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    customer_pass_1 = (
        pl.scan_parquet(customer_files)
        .select(
            pl.lit("customer").alias("entity_type"),
            pl.col("customer_id").alias("natural_key"),
            _customer_key_expr(pl),
        )
        .collect()
    )

    customer_pass_2 = (
        pl.scan_parquet(customer_files)
        .select(
            pl.col("customer_id").alias("natural_key"),
            _customer_key_expr(pl).alias("surrogate_key_pass_2"),
        )
        .collect()
    )

    account_pass_1 = (
        pl.scan_parquet(account_files)
        .select(
            pl.lit("account").alias("entity_type"),
            pl.col("account_id").alias("natural_key"),
            _account_key_expr(pl),
        )
        .collect()
    )

    account_pass_2 = (
        pl.scan_parquet(account_files)
        .select(
            pl.col("account_id").alias("natural_key"),
            _account_key_expr(pl).alias("surrogate_key_pass_2"),
        )
        .collect()
    )

    customer_stability = (
        customer_pass_1
        .join(customer_pass_2, on="natural_key", how="inner")
        .with_columns(
            (pl.col("surrogate_key") == pl.col("surrogate_key_pass_2")).alias("stable")
        )
    )

    account_stability = (
        account_pass_1
        .join(account_pass_2, on="natural_key", how="inner")
        .with_columns(
            (pl.col("surrogate_key") == pl.col("surrogate_key_pass_2")).alias("stable")
        )
    )

    output_df = pl.concat(
        [
            customer_stability.select(
                "entity_type",
                "natural_key",
                "surrogate_key",
                "stable",
            ),
            account_stability.select(
                "entity_type",
                "natural_key",
                "surrogate_key",
                "stable",
            ),
        ],
        how="vertical",
    )

    write_start = time.perf_counter()
    output_df.write_delta(str(key_registry_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    key_table = DeltaTable(str(key_registry_path))
    key_files = active_file_uris(key_table, key_registry_path)

    validation_df = pl.scan_parquet(key_files).collect()
    validation_row = (
        validation_df.select(
            pl.len().alias("row_count"),
            pl.col("natural_key").null_count().alias("null_natural_key_count"),
            pl.col("surrogate_key").null_count().alias("null_surrogate_key_count"),
            pl.col("stable").null_count().alias("null_stable_flag_count"),
            pl.col("stable").cast(pl.Int64).sum().alias("stable_true_count"),
            pl.concat_str(
                [
                    pl.col("entity_type"),
                    pl.lit(":"),
                    pl.col("natural_key"),
                ]
            ).n_unique().alias("unique_entity_natural_keys"),
            pl.concat_str(
                [
                    pl.col("entity_type"),
                    pl.lit(":"),
                    pl.col("surrogate_key").cast(pl.Utf8),
                ]
            ).n_unique().alias("unique_entity_surrogate_keys"),
        )
        .row(0, named=True)
    )

    customer_validation = (
        validation_df
        .filter(pl.col("entity_type") == "customer")
        .select(
            pl.len().alias("row_count"),
            pl.col("stable").cast(pl.Int64).sum().alias("stable_count"),
        )
        .row(0, named=True)
    )

    account_validation = (
        validation_df
        .filter(pl.col("entity_type") == "account")
        .select(
            pl.len().alias("row_count"),
            pl.col("stable").cast(pl.Int64).sum().alias("stable_count"),
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
    output_file_count = len(list(key_registry_path.glob("*.parquet")))
    expected_rows = customer_rows + account_rows
    delta_roundtrip_ok = (
        rows_out == expected_rows
        and validation_row["null_natural_key_count"] == 0
        and validation_row["null_surrogate_key_count"] == 0
        and validation_row["null_stable_flag_count"] == 0
        and validation_row["stable_true_count"] == expected_rows
        and validation_row["unique_entity_natural_keys"] == expected_rows
        and validation_row["unique_entity_surrogate_keys"] == expected_rows
        and customer_validation["row_count"] == customer_rows
        and customer_validation["stable_count"] == customer_rows
        and account_validation["row_count"] == account_rows
        and account_validation["stable_count"] == account_rows
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
            "Polars Gold stable surrogate key benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
            "; key_strategy=sha256_first_8_bytes"
        ),
        artifacts={
            "customers_input_path": str(customers_path),
            "accounts_input_path": str(accounts_path),
            "customers_fixture_path": str(customers_fixture_path),
            "accounts_fixture_path": str(accounts_fixture_path),
            "output_path": str(key_registry_path),
            "customer_rows": customer_rows,
            "account_rows": account_rows,
            "stable_true_count": validation_row["stable_true_count"],
            "null_natural_key_count": validation_row["null_natural_key_count"],
            "null_surrogate_key_count": validation_row["null_surrogate_key_count"],
            "unique_entity_natural_keys": validation_row["unique_entity_natural_keys"],
            "unique_entity_surrogate_keys": validation_row["unique_entity_surrogate_keys"],
            "implementation_ref": (
                "benchmarks.workloads.gold."
                "gld_04_stable_surrogate_keys_polars:run"
            ),
        },
    )