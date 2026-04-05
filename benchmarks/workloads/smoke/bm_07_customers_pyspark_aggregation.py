"""Real PySpark smoke workload over the mounted customers dataset."""

from __future__ import annotations

from pathlib import Path
import resource
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


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    spark,
    data_root: Path,
) -> BenchmarkExecution:
    """Run a real Spark aggregation over customers.csv."""

    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    customers_path = data_root / "customers.csv"
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType(), False),
            T.StructField("id_number", T.StringType(), True),
            T.StructField("first_name", T.StringType(), True),
            T.StructField("last_name", T.StringType(), True),
            T.StructField("dob", T.StringType(), True),
            T.StructField("gender", T.StringType(), True),
            T.StructField("province", T.StringType(), True),
            T.StructField("income_band", T.StringType(), True),
            T.StructField("segment", T.StringType(), True),
            T.StructField("risk_score", T.IntegerType(), True),
            T.StructField("kyc_status", T.StringType(), True),
            T.StructField("product_flags", T.StringType(), True),
        ]
    )

    cpu_start = _cpu_user_seconds()
    start = time.perf_counter()

    customers = (
        spark.read.option("header", True)
        .schema(schema)
        .csv(str(customers_path))
        .select("province")
    )
    rows_in = customers.count()

    province_counts = (
        customers.groupBy("province")
        .agg(F.count("*").alias("customer_count"))
        .orderBy("province")
        .collect()
    )
    rows_out = len(province_counts)

    elapsed_seconds = time.perf_counter() - start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start
    cpu_pct = (
        (cpu_user_seconds / elapsed_seconds) * 100.0 if elapsed_seconds > 0 else None
    )

    preview = [
        {"province": row["province"], "customer_count": row["customer_count"]}
        for row in province_counts[:3]
    ]

    return BenchmarkExecution(
        workload_id=workload.id,
        engine=engine_name,
        elapsed_seconds=elapsed_seconds,
        peak_memory_mb=_peak_memory_mb(),
        rows_in=rows_in,
        rows_out=rows_out,
        cpu_user_seconds=cpu_user_seconds,
        cpu_pct=cpu_pct,
        delta_write_seconds=None,
        delta_read_seconds=None,
        tmp_peak_mb=None,
        output_file_count=0,
        delta_roundtrip_ok=False,
        notes=(
            "real PySpark customers aggregation by province"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "aggregation": "group_by_province_count",
            "preview": preview,
            "implementation_ref": (
                "benchmarks.workloads.smoke."
                "bm_07_customers_pyspark_aggregation:run"
            ),
        },
    )
