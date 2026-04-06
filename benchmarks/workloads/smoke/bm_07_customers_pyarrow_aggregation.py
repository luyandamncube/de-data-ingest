"""PyArrow-backed smoke workload over the mounted customers dataset."""

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
) -> BenchmarkExecution:
    """Run a real PyArrow aggregation over customers.csv."""

    import pyarrow.csv as csv
    import pyarrow.compute as pc

    customers_path = Path("/benchmarks/data/customers.csv")
    if not customers_path.exists():
        raise FileNotFoundError(f"customers dataset not found: {customers_path}")

    read_options = csv.ReadOptions(use_threads=True)
    parse_options = csv.ParseOptions(delimiter=",")
    convert_options = csv.ConvertOptions(
        include_columns=["customer_id", "province"]
    )

    cpu_start = _cpu_user_seconds()
    start = time.perf_counter()
    customers = csv.read_csv(
        customers_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )
    rows_in = customers.num_rows

    value_counts = pc.value_counts(customers.column("province"))
    province_counts = sorted(
        [
            {
                "province": row["values"],
                "customer_count": row["counts"],
            }
            for row in value_counts.to_pylist()
        ],
        key=lambda item: item["province"] or "",
    )
    rows_out = len(province_counts)

    elapsed_seconds = time.perf_counter() - start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start
    cpu_pct = (
        (cpu_user_seconds / elapsed_seconds) * 100.0 if elapsed_seconds > 0 else None
    )

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
            "pyarrow-backed customers aggregation by province"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(customers_path),
            "aggregation": "group_by_province_count",
            "preview": province_counts[:3],
            "implementation_ref": (
                "benchmarks.workloads.smoke."
                "bm_07_customers_pyarrow_aggregation:run"
            ),
        },
    )
