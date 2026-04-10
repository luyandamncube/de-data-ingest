"""PySpark benchmark workload for SLV_01 customers Silver standardisation."""

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


def _count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def _bronze_column_types():
    import pyarrow as pa

    return {field.name: pa.string() for field in BRONZE_CUSTOMERS_SCHEMA.fields}


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    spark,
    data_root: Path,
) -> BenchmarkExecution:
    """Read Bronze customers Delta, standardise customer types, and write Silver."""

    from deltalake import write_deltalake
    import pyarrow as pa
    import pyarrow.csv as csv
    from pyspark.sql import functions as F

    customers_path = data_root / "customers.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    rows_in = _count_csv_rows(customers_path)
    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    bronze_path = attempt_root / "bronze_customers"
    silver_path = attempt_root / "silver_customers"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    bronze_table = csv.read_csv(
        customers_path,
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

    bronze_customers = spark.read.format("delta").load(str(bronze_path))
    silver_df = bronze_customers.select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.col("id_number").cast("string").alias("id_number"),
        F.col("first_name").cast("string").alias("first_name"),
        F.col("last_name").cast("string").alias("last_name"),
        F.to_date(F.col("dob"), "yyyy-MM-dd").alias("dob"),
        F.col("gender").cast("string").alias("gender"),
        F.col("province").cast("string").alias("province"),
        F.col("income_band").cast("string").alias("income_band"),
        F.col("segment").cast("string").alias("segment"),
        F.col("risk_score").cast("int").alias("risk_score"),
        F.col("kyc_status").cast("string").alias("kyc_status"),
        F.col("product_flags").cast("string").alias("product_flags"),
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
    delta_read_seconds = time.perf_counter() - read_start

    validation_seconds = delta_read_seconds
    cpu_pct = (
        (cpu_user_seconds / core_elapsed_seconds) * 100.0
        if core_elapsed_seconds > 0
        else None
    )
    output_file_count = len(list(silver_path.glob("*.parquet")))
    unique_customer_ids = validation_row["unique_customer_ids"]
    null_dob_count = validation_row["null_dob_count"]
    null_risk_score_count = validation_row["null_risk_score_count"]
    delta_roundtrip_ok = (
        rows_in == rows_out
        and unique_customer_ids == rows_out
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
            "PySpark Silver customers standardisation benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "bronze_fixture_path": str(bronze_path),
            "output_path": str(silver_path),
            "unique_customer_ids": unique_customer_ids,
            "null_dob_count": null_dob_count,
            "null_risk_score_count": null_risk_score_count,
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_01_customers_standardise_pyspark:run"
            ),
        },
    )
