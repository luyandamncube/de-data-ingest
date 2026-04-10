"""Polars benchmark workload for SLV_07 transactions currency standardisation."""

from __future__ import annotations

from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.silver.slv_07_transactions_currency_standardise_common import (
    CANONICAL_CURRENCY,
    CURRENCY_VARIANT_FRACTION,
    active_file_uris,
    build_currency_variants_fixture,
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


def _canonical_currency_expr(pl):
    currency_text = pl.col("currency").cast(pl.Utf8).str.strip_chars()

    return (
        pl.when(currency_text.str.to_lowercase().is_in(["zar", "r", "rands", "710"]))
        .then(pl.lit(CANONICAL_CURRENCY))
        .otherwise(currency_text.str.to_uppercase())
    )


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Read Silver-like transactions, standardise currency values, and write Silver."""

    from deltalake import DeltaTable
    import polars as pl

    transactions_path = data_root / "transactions.jsonl"
    if not transactions_path.exists():
        raise FileNotFoundError(
            f"transactions dataset not found: {transactions_path}"
        )

    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    fixture_path = attempt_root / "silver_like_transactions_currency_variants"
    standardised_path = attempt_root / "silver_transactions_currency_standardised"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    base_rows, variant_rows, rows_in = build_currency_variants_fixture(
        transactions_path=transactions_path,
        output_path=fixture_path,
    )

    fixture_table = DeltaTable(str(fixture_path))
    fixture_files = active_file_uris(fixture_table, fixture_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    standardised_df = (
        pl.scan_parquet(fixture_files)
        .with_columns(_canonical_currency_expr(pl).alias("currency"))
        .collect()
    )

    write_start = time.perf_counter()
    standardised_df.write_delta(str(standardised_path), mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    standardised_table = DeltaTable(str(standardised_path))
    standardised_files = active_file_uris(standardised_table, standardised_path)

    validation_row = (
        pl.scan_parquet(standardised_files)
        .select(
            pl.len().alias("row_count"),
            pl.col("currency").null_count().alias("null_currency_count"),
            pl.col("currency")
            .filter(pl.col("currency") != CANONICAL_CURRENCY)
            .len()
            .alias("non_canonical_currency_count"),
        )
        .collect()
        .row(0, named=True)
    )

    distinct_currencies = (
        pl.scan_parquet(standardised_files)
        .select(pl.col("currency").drop_nulls().unique().sort())
        .collect()
        .get_column("currency")
        .to_list()
    )

    rows_out = validation_row["row_count"]
    delta_read_seconds = time.perf_counter() - read_start

    validation_seconds = delta_read_seconds
    cpu_pct = (
        (cpu_user_seconds / core_elapsed_seconds) * 100.0
        if core_elapsed_seconds > 0
        else None
    )
    output_file_count = len(list(standardised_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_out == base_rows
        and validation_row["null_currency_count"] == 0
        and validation_row["non_canonical_currency_count"] == 0
        and distinct_currencies == [CANONICAL_CURRENCY]
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
            "Polars Silver transactions currency standardisation benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
            f"; currency_variant_fraction={CURRENCY_VARIANT_FRACTION:.2f}"
        ),
        artifacts={
            "input_path": str(transactions_path),
            "fixture_path": str(fixture_path),
            "output_path": str(standardised_path),
            "base_rows": base_rows,
            "variant_rows": variant_rows,
            "distinct_currencies": distinct_currencies,
            "null_currency_count": validation_row["null_currency_count"],
            "non_canonical_currency_count": validation_row["non_canonical_currency_count"],
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_07_transactions_currency_standardise_polars:run"
            ),
        },
    )