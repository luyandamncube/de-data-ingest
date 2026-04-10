"""PySpark benchmark workload for SLV_03 transactions Silver standardisation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.silver.slv_03_transactions_common import (
    build_bronze_transactions_table,
    count_jsonl_rows,
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
    spark,
    data_root: Path,
) -> BenchmarkExecution:
    """Read Bronze transactions Delta, standardise types, and write Silver."""

    from deltalake import write_deltalake
    from pyspark.sql import functions as F

    transactions_path = data_root / "transactions.jsonl"
    if not transactions_path.exists():
        raise FileNotFoundError(
            f"transactions dataset not found: {transactions_path}"
        )

    rows_in = count_jsonl_rows(transactions_path)
    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    bronze_path = attempt_root / "bronze_transactions"
    silver_path = attempt_root / "silver_transactions"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    bronze_table = build_bronze_transactions_table(transactions_path, run_timestamp)
    write_deltalake(str(bronze_path), bronze_table, mode="overwrite")

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    bronze_transactions = spark.read.format("delta").load(str(bronze_path))
    silver_df = bronze_transactions.select(
        F.col("transaction_id").cast("string").alias("transaction_id"),
        F.col("account_id").cast("string").alias("account_id"),
        F.to_date(F.col("transaction_date"), "yyyy-MM-dd").alias("transaction_date"),
        F.col("transaction_time").cast("string").alias("transaction_time"),
        F.to_timestamp(
            F.concat_ws(" ", F.col("transaction_date"), F.col("transaction_time")),
            "yyyy-MM-dd HH:mm:ss",
        ).alias("transaction_timestamp"),
        F.col("transaction_type").cast("string").alias("transaction_type"),
        F.col("merchant_category").cast("string").alias("merchant_category"),
        F.col("merchant_subcategory").cast("string").alias("merchant_subcategory"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.col("currency").cast("string")).alias("currency"),
        F.col("channel").cast("string").alias("channel"),
        F.col("location.province").cast("string").alias("province"),
        F.col("location.city").cast("string").alias("city"),
        F.col("location.coordinates").cast("string").alias("coordinates"),
        F.col("metadata.device_id").cast("string").alias("device_id"),
        F.col("metadata.session_id").cast("string").alias("session_id"),
        F.col("metadata.retry_flag").cast("boolean").alias("retry_flag"),
        F.lit(None).cast("string").alias("dq_flag"),
        F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
    )

    write_start = time.perf_counter()
    (
        silver_df.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .save(str(silver_path))
    )
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    roundtrip_df = spark.read.format("delta").load(str(silver_path))
    validation_row = (
        roundtrip_df.agg(
            F.count("*").alias("row_count"),
            F.countDistinct("transaction_id").alias("unique_transaction_ids"),
            F.sum(
                F.when(F.col("transaction_date").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("null_transaction_date_count"),
            F.sum(
                F.when(F.col("transaction_timestamp").isNull(), F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).alias("null_transaction_timestamp_count"),
            F.sum(
                F.when(F.col("amount").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("null_amount_count"),
            F.sum(
                F.when(F.col("currency").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("null_currency_count"),
            F.sum(
                F.when(F.col("dq_flag").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("null_dq_flag_count"),
        ).collect()[0]
    )
    rows_out = validation_row["row_count"]
    unique_transaction_ids = validation_row["unique_transaction_ids"]
    null_transaction_date_count = validation_row["null_transaction_date_count"]
    null_transaction_timestamp_count = validation_row[
        "null_transaction_timestamp_count"
    ]
    null_amount_count = validation_row["null_amount_count"]
    null_currency_count = validation_row["null_currency_count"]
    null_dq_flag_count = validation_row["null_dq_flag_count"]
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
        and unique_transaction_ids == rows_out
        and null_transaction_date_count == 0
        and null_transaction_timestamp_count == 0
        and null_amount_count == 0
        and null_currency_count == 0
        and null_dq_flag_count == rows_out
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
            "PySpark Silver transactions standardisation benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(transactions_path),
            "bronze_fixture_path": str(bronze_path),
            "output_path": str(silver_path),
            "unique_transaction_ids": unique_transaction_ids,
            "null_transaction_date_count": null_transaction_date_count,
            "null_transaction_timestamp_count": null_transaction_timestamp_count,
            "null_amount_count": null_amount_count,
            "null_currency_count": null_currency_count,
            "null_dq_flag_count": null_dq_flag_count,
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_03_transactions_standardise_pyspark:run"
            ),
        },
    )
