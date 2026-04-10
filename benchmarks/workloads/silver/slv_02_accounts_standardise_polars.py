"""Polars benchmark workload for SLV_02 accounts Silver standardisation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from pipeline.registry import TrackingUnit
from pipeline.schemas import BRONZE_ACCOUNTS_SCHEMA
from pyspark.sql import types as T


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


def _polars_dtype(data_type: T.DataType):
    import polars as pl

    if isinstance(data_type, T.StringType):
        return pl.String
    if isinstance(data_type, T.IntegerType):
        return pl.Int32
    if isinstance(data_type, T.DecimalType):
        return pl.Decimal(data_type.precision, data_type.scale)
    if isinstance(data_type, T.BooleanType):
        return pl.Boolean
    raise TypeError(f"unsupported Polars dtype mapping for {data_type}")


def _bronze_polars_schema() -> dict[str, object]:
    return {
        field.name: _polars_dtype(field.dataType)
        for field in BRONZE_ACCOUNTS_SCHEMA.fields
    }


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
    """Read Bronze accounts Delta, standardise account types, and write Silver."""

    from deltalake import DeltaTable
    import polars as pl

    accounts_path = data_root / "accounts.csv"
    if not accounts_path.exists():
        raise FileNotFoundError(f"accounts dataset not found: {accounts_path}")

    rows_in = _count_csv_rows(accounts_path)
    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    bronze_path = attempt_root / "bronze_accounts"
    silver_path = attempt_root / "silver_accounts"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    bronze_df = pl.read_csv(
        accounts_path,
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
                pl.col("account_id").cast(pl.String),
                pl.col("customer_ref").cast(pl.String),
                pl.col("account_type").cast(pl.String),
                pl.col("account_status").cast(pl.String),
                pl.col("open_date").cast(pl.String).str.strptime(
                    pl.Date,
                    "%Y-%m-%d",
                    strict=False,
                ),
                pl.col("product_tier").cast(pl.String),
                pl.col("mobile_number").cast(pl.String),
                pl.col("digital_channel").cast(pl.String),
                pl.col("credit_limit").cast(pl.Decimal(18, 2), strict=False),
                pl.col("current_balance").cast(pl.Decimal(18, 2), strict=False),
                pl.col("last_activity_date").cast(pl.String).str.strptime(
                    pl.Date,
                    "%Y-%m-%d",
                    strict=False,
                ),
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
            pl.col("account_id").n_unique().alias("unique_account_ids"),
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
    output_file_count = len(list(silver_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_in == rows_out
        and validation_row["unique_account_ids"] == rows_out
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
            "Polars Silver accounts standardisation benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(accounts_path),
            "bronze_fixture_path": str(bronze_path),
            "output_path": str(silver_path),
            "unique_account_ids": validation_row["unique_account_ids"],
            "null_open_date_count": validation_row["null_open_date_count"],
            "null_current_balance_count": validation_row[
                "null_current_balance_count"
            ],
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_02_accounts_standardise_polars:run"
            ),
        },
    )
