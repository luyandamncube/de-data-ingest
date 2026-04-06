"""Benchmark workload for BRZ_01 customers Bronze ingestion."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from pipeline.registry import TrackingUnit
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA


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
    """Read customers.csv, add a run-level timestamp, and write Bronze Delta."""

    from pyspark.sql import functions as F

    customers_path = data_root / "customers.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    output_path = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    shutil.rmtree(output_path, ignore_errors=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cpu_start = _cpu_user_seconds()
    start = time.perf_counter()

    customers_df = (
        spark.read.option("header", True)
        .schema(BRONZE_CUSTOMERS_SCHEMA)
        .csv(str(customers_path))
    )
    rows_in = customers_df.count()

    write_start = time.perf_counter()
    (
        customers_df.withColumn(
            "ingestion_timestamp",
            F.lit(run_timestamp).cast("timestamp"),
        )
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output_path))
    )
    delta_write_seconds = time.perf_counter() - write_start

    read_start = time.perf_counter()
    roundtrip_df = spark.read.format("delta").load(str(output_path))
    rows_out = roundtrip_df.count()
    distinct_timestamps = roundtrip_df.select("ingestion_timestamp").distinct().count()
    delta_read_seconds = time.perf_counter() - read_start

    elapsed_seconds = time.perf_counter() - start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start
    cpu_pct = (
        (cpu_user_seconds / elapsed_seconds) * 100.0 if elapsed_seconds > 0 else None
    )
    output_file_count = len(list(output_path.glob("*.parquet")))
    delta_roundtrip_ok = rows_in == rows_out and distinct_timestamps == 1

    return BenchmarkExecution(
        workload_id=workload.id,
        engine=engine_name,
        elapsed_seconds=elapsed_seconds,
        peak_memory_mb=_peak_memory_mb(),
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
            "PySpark Bronze customers ingest benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "output_path": str(output_path),
            "distinct_ingestion_timestamps": distinct_timestamps,
            "implementation_ref": (
                "benchmarks.workloads.bronze.brz_01_ingest_customers_raw:run"
            ),
        },
    )
