"""PyArrow benchmark workload for SLV_02 accounts Silver standardisation."""

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


def _active_file_uris(delta_table, table_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(table_path / relative_path) for relative_path in delta_table.files()]


def _empty_string_to_null(values):
    import pyarrow as pa
    import pyarrow.compute as pc

    empty_mask = pc.equal(values, "")
    nulls = pa.nulls(len(values), type=pa.string())
    return pc.if_else(empty_mask, nulls, values)


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    data_root: Path,
) -> BenchmarkExecution:
    """Read Bronze accounts Delta, standardise account types, and write Silver."""

    from deltalake import DeltaTable, write_deltalake
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as csv
    import pyarrow.dataset as ds

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

    bronze_delta = DeltaTable(str(bronze_path))
    bronze_files = _active_file_uris(bronze_delta, bronze_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    bronze_dataset = ds.dataset(bronze_files, format="parquet")
    bronze_accounts = bronze_dataset.to_table()
    parsed_open_date = pc.cast(
        pc.strptime(
            bronze_accounts.column("open_date"),
            format="%Y-%m-%d",
            unit="s",
            error_is_null=True,
        ),
        pa.date32(),
    )
    credit_limit = pc.cast(
        _empty_string_to_null(bronze_accounts.column("credit_limit")),
        pa.decimal128(18, 2),
        safe=False,
    )
    current_balance = pc.cast(
        _empty_string_to_null(bronze_accounts.column("current_balance")),
        pa.decimal128(18, 2),
        safe=False,
    )
    parsed_last_activity_date = pc.cast(
        pc.strptime(
            bronze_accounts.column("last_activity_date"),
            format="%Y-%m-%d",
            unit="s",
            error_is_null=True,
        ),
        pa.date32(),
    )
    silver_table = pa.table(
        {
            "account_id": bronze_accounts.column("account_id"),
            "customer_ref": bronze_accounts.column("customer_ref"),
            "account_type": bronze_accounts.column("account_type"),
            "account_status": bronze_accounts.column("account_status"),
            "open_date": parsed_open_date,
            "product_tier": bronze_accounts.column("product_tier"),
            "mobile_number": bronze_accounts.column("mobile_number"),
            "digital_channel": bronze_accounts.column("digital_channel"),
            "credit_limit": credit_limit,
            "current_balance": current_balance,
            "last_activity_date": parsed_last_activity_date,
            "ingestion_timestamp": bronze_accounts.column("ingestion_timestamp"),
        }
    )

    write_start = time.perf_counter()
    write_deltalake(str(silver_path), silver_table, mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    silver_delta = DeltaTable(str(silver_path))
    silver_files = _active_file_uris(silver_delta, silver_path)
    roundtrip_table = ds.dataset(silver_files, format="parquet").to_table(
        columns=["account_id", "open_date", "current_balance"]
    )
    rows_out = roundtrip_table.num_rows
    account_ids = roundtrip_table.column("account_id").combine_chunks()
    unique_account_ids = len(pc.unique(account_ids))
    null_open_date_count = pc.sum(
        pc.is_null(roundtrip_table.column("open_date"))
    ).as_py()
    null_current_balance_count = pc.sum(
        pc.is_null(roundtrip_table.column("current_balance"))
    ).as_py()
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
            "PyArrow Silver accounts standardisation benchmark"
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
                "slv_02_accounts_standardise_pyarrow:run"
            ),
        },
    )
