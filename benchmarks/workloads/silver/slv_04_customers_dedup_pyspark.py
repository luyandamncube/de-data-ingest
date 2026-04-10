"""PySpark benchmark workload for SLV_04 customers Silver deduplication."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.silver.slv_04_customers_dedup_common import (
    DEDUP_FRACTION,
    build_duplicate_customers_fixture,
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
    """Read duplicated Silver-like customers Delta, deduplicate, and write Silver."""

    from pyspark.sql import Window
    from pyspark.sql import functions as F

    customers_path = data_root / "customers.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    fixture_path = attempt_root / "silver_like_customers_with_dups"
    deduped_path = attempt_root / "silver_customers_deduped"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    base_rows, duplicate_rows, rows_in = build_duplicate_customers_fixture(
        customers_path=customers_path,
        output_path=fixture_path,
        run_timestamp=run_timestamp,
    )

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    fixture_df = spark.read.format("delta").load(str(fixture_path))
    window = Window.partitionBy("customer_id").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    deduped_df = (
        fixture_df.withColumn("__row_number", F.row_number().over(window))
        .where(F.col("__row_number") == 1)
        .drop("__row_number")
    )

    write_start = time.perf_counter()
    (
        deduped_df.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .save(str(deduped_path))
    )
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    roundtrip_df = spark.read.format("delta").load(str(deduped_path))
    validation_row = (
        roundtrip_df.agg(
            F.count("*").alias("row_count"),
            F.countDistinct("customer_id").alias("unique_customer_ids"),
            F.sum(F.when(F.col("dob").isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
                "null_dob_count"
            ),
            F.sum(
                F.when(F.col("risk_score").isNull(), F.lit(1)).otherwise(F.lit(0))
            ).alias("null_risk_score_count"),
        ).collect()[0]
    )
    rows_out = validation_row["row_count"]
    unique_customer_ids = validation_row["unique_customer_ids"]
    null_dob_count = validation_row["null_dob_count"]
    null_risk_score_count = validation_row["null_risk_score_count"]
    delta_read_seconds = time.perf_counter() - read_start

    validation_seconds = delta_read_seconds
    cpu_pct = (
        (cpu_user_seconds / core_elapsed_seconds) * 100.0
        if core_elapsed_seconds > 0
        else None
    )
    output_file_count = len(list(deduped_path.glob("*.parquet")))
    delta_roundtrip_ok = (
        rows_out == base_rows
        and unique_customer_ids == base_rows
        and null_dob_count == 0
        and null_risk_score_count == 0
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
            "PySpark Silver customers dedup benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
            f"; duplicate_fraction={DEDUP_FRACTION:.2f}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "fixture_path": str(fixture_path),
            "output_path": str(deduped_path),
            "base_rows": base_rows,
            "duplicate_rows": duplicate_rows,
            "unique_customer_ids": unique_customer_ids,
            "null_dob_count": null_dob_count,
            "null_risk_score_count": null_risk_score_count,
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_04_customers_dedup_pyspark:run"
            ),
        },
    )
