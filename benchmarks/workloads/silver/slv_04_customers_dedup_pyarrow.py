"""PyArrow benchmark workload for SLV_04 customers Silver deduplication."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.silver.slv_04_customers_dedup_common import (
    DEDUP_FRACTION,
    active_file_uris,
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
    data_root: Path,
) -> BenchmarkExecution:
    """Read duplicated Silver-like customers Delta, deduplicate, and write Silver."""

    from deltalake import DeltaTable, write_deltalake
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

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

    fixture_delta = DeltaTable(str(fixture_path))
    fixture_files = active_file_uris(fixture_delta, fixture_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    raw_fixture_table = ds.dataset(fixture_files, format="parquet").to_table()
    fixture_table = pa.table(
        {
            "customer_id": pc.cast(
                raw_fixture_table.column("customer_id"),
                pa.string(),
            ),
            "id_number": pc.cast(
                raw_fixture_table.column("id_number"),
                pa.string(),
            ),
            "first_name": pc.cast(
                raw_fixture_table.column("first_name"),
                pa.string(),
            ),
            "last_name": pc.cast(
                raw_fixture_table.column("last_name"),
                pa.string(),
            ),
            "dob": raw_fixture_table.column("dob"),
            "gender": pc.cast(
                raw_fixture_table.column("gender"),
                pa.string(),
            ),
            "province": pc.cast(
                raw_fixture_table.column("province"),
                pa.string(),
            ),
            "income_band": pc.cast(
                raw_fixture_table.column("income_band"),
                pa.string(),
            ),
            "segment": pc.cast(
                raw_fixture_table.column("segment"),
                pa.string(),
            ),
            "risk_score": raw_fixture_table.column("risk_score"),
            "kyc_status": pc.cast(
                raw_fixture_table.column("kyc_status"),
                pa.string(),
            ),
            "product_flags": pc.cast(
                raw_fixture_table.column("product_flags"),
                pa.string(),
            ),
            "ingestion_timestamp": pc.cast(
                raw_fixture_table.column("ingestion_timestamp"),
                pa.timestamp("us"),
            ),
        }
    )
    sort_key_table = pa.table(
        {
            "customer_id": fixture_table.column("customer_id"),
            "ingestion_timestamp": fixture_table.column("ingestion_timestamp"),
        }
    )
    sort_indices = pc.sort_indices(
        sort_key_table,
        sort_keys=[
            ("customer_id", "ascending"),
            ("ingestion_timestamp", "descending"),
        ],
    )
    sorted_table = fixture_table.take(sort_indices)
    customer_ids = sorted_table.column("customer_id").combine_chunks().to_pylist()
    keep_indices: list[int] = []
    previous_customer_id = None
    for position, customer_id in enumerate(customer_ids):
        if customer_id != previous_customer_id:
            keep_indices.append(position)
            previous_customer_id = customer_id
    deduped_table = sorted_table.take(pa.array(keep_indices, type=pa.int64()))

    write_start = time.perf_counter()
    write_deltalake(str(deduped_path), deduped_table, mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    deduped_delta = DeltaTable(str(deduped_path))
    deduped_files = active_file_uris(deduped_delta, deduped_path)
    roundtrip_table = ds.dataset(deduped_files, format="parquet").to_table(
        columns=["customer_id", "dob", "risk_score"]
    )
    rows_out = roundtrip_table.num_rows
    unique_customer_ids = len(
        pc.unique(roundtrip_table.column("customer_id").combine_chunks())
    )
    null_dob_count = pc.sum(pc.is_null(roundtrip_table.column("dob"))).as_py()
    null_risk_score_count = pc.sum(
        pc.is_null(roundtrip_table.column("risk_score"))
    ).as_py()
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
            "PyArrow Silver customers dedup benchmark"
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
                "slv_04_customers_dedup_pyarrow:run"
            ),
        },
    )
