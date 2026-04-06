"""PyArrow benchmark workload for BRZ_02 accounts Bronze ingestion."""

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


def _arrow_column_types():
    import pyarrow as pa

    return {field.name: pa.string() for field in BRONZE_ACCOUNTS_SCHEMA.fields}


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Read accounts.csv, add a run-level timestamp, and write Bronze Delta."""

    from deltalake import DeltaTable, write_deltalake
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as csv
    import pyarrow.dataset as ds

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

    accounts_table = csv.read_csv(
        accounts_path,
        read_options=csv.ReadOptions(use_threads=True),
        parse_options=csv.ParseOptions(delimiter=","),
        convert_options=csv.ConvertOptions(column_types=_arrow_column_types()),
    )
    ingestion_column = pa.array(
        [run_timestamp] * accounts_table.num_rows,
        type=pa.timestamp("us"),
    )
    accounts_with_ts = accounts_table.append_column(
        "ingestion_timestamp",
        ingestion_column,
    )

    write_start = time.perf_counter()
    write_deltalake(str(output_path), accounts_with_ts, mode="overwrite")
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
    roundtrip_table = ds.dataset(active_files, format="parquet").to_table(
        columns=["ingestion_timestamp"]
    )
    rows_out = roundtrip_table.num_rows
    timestamp_column = roundtrip_table.column("ingestion_timestamp").combine_chunks()
    min_ingestion_timestamp = pc.min(timestamp_column).as_py()
    max_ingestion_timestamp = pc.max(timestamp_column).as_py()
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
            "PyArrow Bronze accounts ingest benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(accounts_path),
            "output_path": str(output_path),
            "min_ingestion_timestamp": str(min_ingestion_timestamp),
            "max_ingestion_timestamp": str(max_ingestion_timestamp),
            "implementation_ref": (
                "benchmarks.workloads.bronze."
                "brz_02_ingest_accounts_raw_pyarrow:run"
            ),
        },
    )
