"""Benchmark workload for BRZ_02 accounts Bronze ingestion."""

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


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    spark,
    data_root: Path,
) -> BenchmarkExecution:
    """Read accounts.csv, add a run-level timestamp, and write Bronze Delta."""

    from pyspark.sql import functions as F

    accounts_path = data_root / "accounts.csv"
    if not accounts_path.exists():
        raise FileNotFoundError(f"accounts dataset not found: {accounts_path}")

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    rows_in = _count_csv_rows(accounts_path)
    output_path = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    shutil.rmtree(output_path, ignore_errors=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    accounts_df = (
        spark.read.option("header", True)
        .schema(BRONZE_ACCOUNTS_SCHEMA)
        .csv(str(accounts_path))
    )
    accounts_with_ts = accounts_df.withColumn(
        "ingestion_timestamp",
        F.lit(run_timestamp).cast("timestamp"),
    )

    write_start = time.perf_counter()
    (
        accounts_with_ts.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .save(str(output_path))
    )
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    roundtrip_df = spark.read.format("delta").load(str(output_path))
    validation_row = (
        roundtrip_df.agg(
            F.count("*").alias("row_count"),
            F.min("ingestion_timestamp").alias("min_ingestion_timestamp"),
            F.max("ingestion_timestamp").alias("max_ingestion_timestamp"),
        ).collect()[0]
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
            "PySpark Bronze accounts ingest benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(accounts_path),
            "output_path": str(output_path),
            "min_ingestion_timestamp": str(min_ingestion_timestamp),
            "max_ingestion_timestamp": str(max_ingestion_timestamp),
            "implementation_ref": (
                "benchmarks.workloads.bronze.brz_02_ingest_accounts_raw:run"
            ),
        },
    )
