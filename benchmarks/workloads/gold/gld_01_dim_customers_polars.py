"""Polars benchmark workload for GLD_01 dim_customers Gold build."""

from __future__ import annotations

from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.gold.gld_01_dim_customers_common import (
    active_file_uris,
    build_dim_customers_fixture,
)
from pipeline.registry import TrackingUnit


AS_OF_YEAR = 2025


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


def _age_band_expr(pl):
    dob_date = pl.col("dob").str.strptime(pl.Date, format="%Y-%m-%d", strict=True)
    age_years = pl.lit(AS_OF_YEAR) - dob_date.dt.year()

    return (
        pl.when(age_years < 25)
        .then(pl.lit("18_24"))
        .when(age_years < 35)
        .then(pl.lit("25_34"))
        .when(age_years < 45)
        .then(pl.lit("35_44"))
        .when(age_years < 55)
        .then(pl.lit("45_54"))
        .when(age_years < 65)
        .then(pl.lit("55_64"))
        .otherwise(pl.lit("65_PLUS"))
    )


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Build a Gold dim_customers output from the customers source."""

    from deltalake import DeltaTable
    import polars as pl

    customers_path = data_root / "customers.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    fixture_path = attempt_root / "silver_like_customers"
    dim_path = attempt_root / "gold_dim_customers"

    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    base_rows, rows_in = build_dim_customers_fixture(
        customers_path=customers_path,
        output_path=fixture_path,
    )

    fixture_table = DeltaTable(str(fixture_path))
    fixture_files = active_file_uris(fixture_table, fixture_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    dim_df = (
        pl.scan_parquet(fixture_files)
        .with_columns(_age_band_expr(pl).alias("age_band"))
        .select(
            "customer_id",
            "id_number",
            "first_name",
            "last_name",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "product_flags",
            "age_band",
        )
        .collect()
        .sort("customer_id")
        .with_row_index("customer_sk", offset=1)
        .with_columns(pl.col("customer_sk").cast(pl.Int64))
        .select(
            "customer_sk",
            "customer_id",
            "id_number",
            "first_name",
            "last_name",
            "gender",
            "province",
            "income_band",
            "segment",
            "risk_score",
            "kyc_status",
            "product_flags",
            "age_band",
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
            pl.col("customer_sk").n_unique().alias("unique_customer_sks"),
            pl.col("customer_id").n_unique().alias("unique_customer_ids"),
            pl.col("customer_sk").null_count().alias("null_customer_sk_count"),
            pl.col("customer_id").null_count().alias("null_customer_id_count"),
            pl.col("age_band").null_count().alias("null_age_band_count"),
        )
        .row(0, named=True)
    )
    has_dob_column = "dob" in validation_df.columns
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
        and validation_row["unique_customer_sks"] == base_rows
        and validation_row["unique_customer_ids"] == base_rows
        and validation_row["null_customer_sk_count"] == 0
        and validation_row["null_customer_id_count"] == 0
        and validation_row["null_age_band_count"] == 0
        and not has_dob_column
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
            "Polars Gold dim_customers benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
            f"; as_of_year={AS_OF_YEAR}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "fixture_path": str(fixture_path),
            "output_path": str(dim_path),
            "base_rows": base_rows,
            "output_columns": validation_df.columns,
            "has_dob_column": has_dob_column,
            "unique_customer_sks": validation_row["unique_customer_sks"],
            "unique_customer_ids": validation_row["unique_customer_ids"],
            "null_customer_sk_count": validation_row["null_customer_sk_count"],
            "null_customer_id_count": validation_row["null_customer_id_count"],
            "null_age_band_count": validation_row["null_age_band_count"],
            "implementation_ref": (
                "benchmarks.workloads.gold."
                "gld_01_dim_customers_polars:run"
            ),
        },
    )