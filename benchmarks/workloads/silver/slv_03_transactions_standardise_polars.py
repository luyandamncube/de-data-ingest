"""Polars benchmark workload for SLV_03 transactions Silver standardisation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
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


def _count_jsonl_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def _active_file_uris(delta_table, table_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(table_path / relative_path) for relative_path in delta_table.files()]


def _build_bronze_transactions_frame(transactions_path: Path, run_timestamp):
    import polars as pl

    transactions_df = pl.read_ndjson(transactions_path)
    merchant_subcategory = (
        pl.col("merchant_subcategory").cast(pl.String)
        if "merchant_subcategory" in transactions_df.columns
        else pl.lit(None, dtype=pl.String)
    )

    return (
        transactions_df.with_columns(
            [
                pl.col("transaction_id").cast(pl.String),
                pl.col("account_id").cast(pl.String),
                pl.col("transaction_date").cast(pl.String),
                pl.col("transaction_time").cast(pl.String),
                pl.col("transaction_type").cast(pl.String),
                pl.col("merchant_category").cast(pl.String),
                merchant_subcategory.alias("merchant_subcategory"),
                pl.col("amount").cast(pl.Decimal(18, 2), strict=False),
                pl.col("currency").cast(pl.String),
                pl.col("channel").cast(pl.String),
                pl.struct(
                    pl.col("location").struct.field("province").cast(pl.String),
                    pl.col("location").struct.field("city").cast(pl.String),
                    pl.col("location").struct.field("coordinates").cast(pl.String),
                ).alias("location"),
                pl.struct(
                    pl.col("metadata").struct.field("device_id").cast(pl.String),
                    pl.col("metadata").struct.field("session_id").cast(pl.String),
                    pl.col("metadata").struct.field("retry_flag").cast(pl.Boolean),
                ).alias("metadata"),
                pl.lit(run_timestamp)
                .cast(pl.Datetime("us"))
                .alias("ingestion_timestamp"),
            ]
        )
        .select(
            [
                "transaction_id",
                "account_id",
                "transaction_date",
                "transaction_time",
                "transaction_type",
                "merchant_category",
                "merchant_subcategory",
                "amount",
                "currency",
                "channel",
                "location",
                "metadata",
                "ingestion_timestamp",
            ]
        )
    )


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Read Bronze transactions Delta, standardise types, and write Silver."""

    from deltalake import DeltaTable
    import polars as pl

    transactions_path = data_root / "transactions.jsonl"
    if not transactions_path.exists():
        raise FileNotFoundError(
            f"transactions dataset not found: {transactions_path}"
        )

    rows_in = _count_jsonl_rows(transactions_path)
    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    bronze_path = attempt_root / "bronze_transactions"
    silver_path = attempt_root / "silver_transactions"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    bronze_df = _build_bronze_transactions_frame(transactions_path, run_timestamp)
    bronze_df.write_delta(str(bronze_path), mode="overwrite")

    bronze_table = DeltaTable(str(bronze_path))
    bronze_files = _active_file_uris(bronze_table, bronze_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    transaction_timestamp_expr = (
        pl.concat_str(
            [
                pl.col("transaction_date").cast(pl.String),
                pl.lit(" "),
                pl.col("transaction_time").cast(pl.String),
            ]
        )
        .str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S", strict=False)
        .alias("transaction_timestamp")
    )

    silver_df = (
        pl.scan_parquet(bronze_files)
        .select(
            [
                pl.col("transaction_id").cast(pl.String),
                pl.col("account_id").cast(pl.String),
                pl.col("transaction_date").cast(pl.String).str.strptime(
                    pl.Date,
                    "%Y-%m-%d",
                    strict=False,
                ),
                pl.col("transaction_time").cast(pl.String),
                transaction_timestamp_expr,
                pl.col("transaction_type").cast(pl.String),
                pl.col("merchant_category").cast(pl.String),
                pl.col("merchant_subcategory").cast(pl.String),
                pl.col("amount").cast(pl.Decimal(18, 2), strict=False),
                pl.col("currency").cast(pl.String).str.to_uppercase(),
                pl.col("channel").cast(pl.String),
                pl.col("location").struct.field("province").cast(pl.String).alias(
                    "province"
                ),
                pl.col("location").struct.field("city").cast(pl.String).alias("city"),
                pl.col("location")
                .struct.field("coordinates")
                .cast(pl.String)
                .alias("coordinates"),
                pl.col("metadata").struct.field("device_id").cast(pl.String).alias(
                    "device_id"
                ),
                pl.col("metadata").struct.field("session_id").cast(pl.String).alias(
                    "session_id"
                ),
                pl.col("metadata")
                .struct.field("retry_flag")
                .cast(pl.Boolean)
                .alias("retry_flag"),
                pl.lit(None, dtype=pl.String).alias("dq_flag"),
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
            pl.col("transaction_id").n_unique().alias("unique_transaction_ids"),
            pl.col("transaction_date").null_count().alias("null_transaction_date_count"),
            pl.col("transaction_timestamp")
            .null_count()
            .alias("null_transaction_timestamp_count"),
            pl.col("amount").null_count().alias("null_amount_count"),
            pl.col("currency").null_count().alias("null_currency_count"),
            pl.col("dq_flag").null_count().alias("null_dq_flag_count"),
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
        and validation_row["unique_transaction_ids"] == rows_out
        and validation_row["null_transaction_date_count"] == 0
        and validation_row["null_transaction_timestamp_count"] == 0
        and validation_row["null_amount_count"] == 0
        and validation_row["null_currency_count"] == 0
        and validation_row["null_dq_flag_count"] == rows_out
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
            "Polars Silver transactions standardisation benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(transactions_path),
            "bronze_fixture_path": str(bronze_path),
            "output_path": str(silver_path),
            "unique_transaction_ids": validation_row["unique_transaction_ids"],
            "null_transaction_date_count": validation_row[
                "null_transaction_date_count"
            ],
            "null_transaction_timestamp_count": validation_row[
                "null_transaction_timestamp_count"
            ],
            "null_amount_count": validation_row["null_amount_count"],
            "null_currency_count": validation_row["null_currency_count"],
            "null_dq_flag_count": validation_row["null_dq_flag_count"],
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_03_transactions_standardise_polars:run"
            ),
        },
    )
