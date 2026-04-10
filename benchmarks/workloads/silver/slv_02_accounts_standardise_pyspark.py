"""PySpark benchmark workload for SLV_02 accounts Silver standardisation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from pipeline.registry import TrackingUnit
from pipeline.schemas import BRONZE_ACCOUNTS_SCHEMA


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


def _bronze_column_types():
    import pyarrow as pa

    return {field.name: pa.string() for field in BRONZE_ACCOUNTS_SCHEMA.fields}


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    spark,
    data_root: Path,
) -> BenchmarkExecution:
    """Read Bronze accounts Delta, standardise account types, and write Silver."""

    from deltalake import write_deltalake
    import pyarrow as pa
    import pyarrow.csv as csv
    from pyspark.sql import functions as F

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
    bronze_table = csv.read_csv(
        accounts_path,
        read_options=csv.ReadOptions(use_threads=True),
        parse_options=csv.ParseOptions(delimiter=","),
        convert_options=csv.ConvertOptions(column_types=_bronze_column_types()),
    )
    bronze_with_ts = bronze_table.append_column(
        "ingestion_timestamp",
        pa.array([run_timestamp] * bronze_table.num_rows, type=pa.timestamp("us")),
    )
    write_deltalake(str(bronze_path), bronze_with_ts, mode="overwrite")

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    bronze_accounts = spark.read.format("delta").load(str(bronze_path))
    silver_df = bronze_accounts.select(
        F.col("account_id").cast("string").alias("account_id"),
        F.col("customer_ref").cast("string").alias("customer_ref"),
        F.col("account_type").cast("string").alias("account_type"),
        F.col("account_status").cast("string").alias("account_status"),
        F.to_date(F.col("open_date"), "yyyy-MM-dd").alias("open_date"),
        F.col("product_tier").cast("string").alias("product_tier"),
        F.col("mobile_number").cast("string").alias("mobile_number"),
        F.col("digital_channel").cast("string").alias("digital_channel"),
        F.col("credit_limit").cast("decimal(18,2)").alias("credit_limit"),
        F.col("current_balance").cast("decimal(18,2)").alias("current_balance"),
        F.to_date(F.col("last_activity_date"), "yyyy-MM-dd").alias(
            "last_activity_date"
        ),
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
            F.countDistinct("account_id").alias("unique_account_ids"),
            F.sum(
                F.when(F.col("open_date").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("null_open_date_count"),
            F.sum(
                F.when(F.col("current_balance").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("null_current_balance_count"),
        ).collect()[0]
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
    unique_account_ids = validation_row["unique_account_ids"]
    null_open_date_count = validation_row["null_open_date_count"]
    null_current_balance_count = validation_row["null_current_balance_count"]
    delta_roundtrip_ok = (
        rows_in == rows_out
        and unique_account_ids == rows_out
        and null_open_date_count == 0
        and null_current_balance_count == 0
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
            "PySpark Silver accounts standardisation benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(accounts_path),
            "bronze_fixture_path": str(bronze_path),
            "output_path": str(silver_path),
            "unique_account_ids": unique_account_ids,
            "null_open_date_count": null_open_date_count,
            "null_current_balance_count": null_current_balance_count,
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_02_accounts_standardise_pyspark:run"
            ),
        },
    )
