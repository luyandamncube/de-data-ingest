"""Polars benchmark workload for BRZ_03 transactions Bronze ingestion."""

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


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Read transactions.jsonl, normalise into the Bronze contract, and write Delta."""

    from deltalake import DeltaTable
    import polars as pl

    transactions_path = data_root / "transactions.jsonl"
    if not transactions_path.exists():
        raise FileNotFoundError(
            f"transactions dataset not found: {transactions_path}"
        )

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    rows_in = _count_jsonl_rows(transactions_path)
    output_path = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    shutil.rmtree(output_path, ignore_errors=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    transactions_df = pl.read_ndjson(transactions_path)
    merchant_subcategory = (
        pl.col("merchant_subcategory").cast(pl.String)
        if "merchant_subcategory" in transactions_df.columns
        else pl.lit(None, dtype=pl.String)
    )

    transactions_with_ts = (
        transactions_df.with_columns(
            [
                pl.col("transaction_id").cast(pl.String),
                pl.col("account_id").cast(pl.String),
                pl.col("transaction_date").cast(pl.String),
                pl.col("transaction_time").cast(pl.String),
                pl.col("transaction_type").cast(pl.String),
                pl.col("merchant_category").cast(pl.String),
                merchant_subcategory.alias("merchant_subcategory"),
                pl.col("amount").cast(pl.String),
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
                    pl.col("metadata").struct.field("retry_flag").cast(pl.String),
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

    write_start = time.perf_counter()
    transactions_with_ts.write_delta(str(output_path), mode="overwrite")
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
            "Polars Bronze transactions ingest benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(transactions_path),
            "output_path": str(output_path),
            "min_ingestion_timestamp": str(min_ingestion_timestamp),
            "max_ingestion_timestamp": str(max_ingestion_timestamp),
            "implementation_ref": (
                "benchmarks.workloads.bronze."
                "brz_03_ingest_transactions_raw_polars:run"
            ),
        },
    )
