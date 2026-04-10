"""PyArrow benchmark workload for SLV_03 transactions Silver standardisation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import resource
import shutil
import time

from benchmarks.adapters.base import BenchmarkExecution
from benchmarks.workloads.silver.slv_03_transactions_common import (
    active_file_uris,
    build_bronze_transactions_table,
    count_jsonl_rows,
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
    """Read Bronze transactions Delta, standardise types, and write Silver."""

    from deltalake import DeltaTable, write_deltalake
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.dataset as ds

    transactions_path = data_root / "transactions.jsonl"
    if not transactions_path.exists():
        raise FileNotFoundError(
            f"transactions dataset not found: {transactions_path}"
        )

    rows_in = count_jsonl_rows(transactions_path)
    attempt_root = Path("/tmp") / "benchmarks" / workload.id.lower() / f"attempt_{attempt}"
    bronze_path = attempt_root / "bronze_transactions"
    silver_path = attempt_root / "silver_transactions"
    shutil.rmtree(attempt_root, ignore_errors=True)
    attempt_root.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    bronze_table = build_bronze_transactions_table(transactions_path, run_timestamp)
    write_deltalake(str(bronze_path), bronze_table, mode="overwrite")

    bronze_delta = DeltaTable(str(bronze_path))
    bronze_files = active_file_uris(bronze_delta, bronze_path)

    cpu_start = _cpu_user_seconds()
    core_start = time.perf_counter()

    bronze_transactions = ds.dataset(bronze_files, format="parquet").to_table()
    parsed_transaction_date = pc.cast(
        pc.strptime(
            bronze_transactions.column("transaction_date"),
            format="%Y-%m-%d",
            unit="s",
            error_is_null=True,
        ),
        pa.date32(),
    )
    transaction_timestamp = pc.cast(
        pc.strptime(
            pc.binary_join_element_wise(
                bronze_transactions.column("transaction_date"),
                bronze_transactions.column("transaction_time"),
                " ",
            ),
            format="%Y-%m-%d %H:%M:%S",
            unit="s",
            error_is_null=True,
        ),
        pa.timestamp("us"),
    )
    silver_table = pa.table(
        {
            "transaction_id": bronze_transactions.column("transaction_id"),
            "account_id": bronze_transactions.column("account_id"),
            "transaction_date": parsed_transaction_date,
            "transaction_time": bronze_transactions.column("transaction_time"),
            "transaction_timestamp": transaction_timestamp,
            "transaction_type": bronze_transactions.column("transaction_type"),
            "merchant_category": bronze_transactions.column("merchant_category"),
            "merchant_subcategory": bronze_transactions.column("merchant_subcategory"),
            "amount": pc.cast(
                bronze_transactions.column("amount"),
                pa.decimal128(18, 2),
                safe=False,
            ),
            "currency": pc.utf8_upper(bronze_transactions.column("currency")),
            "channel": bronze_transactions.column("channel"),
            "province": pc.struct_field(bronze_transactions.column("location"), "province"),
            "city": pc.struct_field(bronze_transactions.column("location"), "city"),
            "coordinates": pc.struct_field(
                bronze_transactions.column("location"),
                "coordinates",
            ),
            "device_id": pc.struct_field(
                bronze_transactions.column("metadata"),
                "device_id",
            ),
            "session_id": pc.struct_field(
                bronze_transactions.column("metadata"),
                "session_id",
            ),
            "retry_flag": pc.struct_field(
                bronze_transactions.column("metadata"),
                "retry_flag",
            ),
            "dq_flag": pa.nulls(bronze_transactions.num_rows, type=pa.string()),
            "ingestion_timestamp": bronze_transactions.column("ingestion_timestamp"),
        }
    )

    write_start = time.perf_counter()
    write_deltalake(str(silver_path), silver_table, mode="overwrite")
    delta_write_seconds = time.perf_counter() - write_start

    core_elapsed_seconds = time.perf_counter() - core_start
    cpu_user_seconds = _cpu_user_seconds() - cpu_start

    read_start = time.perf_counter()
    silver_delta = DeltaTable(str(silver_path))
    silver_files = active_file_uris(silver_delta, silver_path)
    roundtrip_table = ds.dataset(silver_files, format="parquet").to_table(
        columns=[
            "transaction_id",
            "transaction_date",
            "transaction_timestamp",
            "amount",
            "currency",
            "dq_flag",
        ]
    )
    rows_out = roundtrip_table.num_rows
    transaction_ids = roundtrip_table.column("transaction_id").combine_chunks()
    unique_transaction_ids = len(pc.unique(transaction_ids))
    null_transaction_date_count = pc.sum(
        pc.is_null(roundtrip_table.column("transaction_date"))
    ).as_py()
    null_transaction_timestamp_count = pc.sum(
        pc.is_null(roundtrip_table.column("transaction_timestamp"))
    ).as_py()
    null_amount_count = pc.sum(
        pc.is_null(roundtrip_table.column("amount"))
    ).as_py()
    null_currency_count = pc.sum(
        pc.is_null(roundtrip_table.column("currency"))
    ).as_py()
    null_dq_flag_count = pc.sum(
        pc.is_null(roundtrip_table.column("dq_flag"))
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
        and unique_transaction_ids == rows_out
        and null_transaction_date_count == 0
        and null_transaction_timestamp_count == 0
        and null_amount_count == 0
        and null_currency_count == 0
        and null_dq_flag_count == rows_out
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
            "PyArrow Silver transactions standardisation benchmark"
            f"; dataset_profile={dataset_profile}"
            f"; attempt={attempt}"
        ),
        artifacts={
            "input_path": str(transactions_path),
            "bronze_fixture_path": str(bronze_path),
            "output_path": str(silver_path),
            "unique_transaction_ids": unique_transaction_ids,
            "null_transaction_date_count": null_transaction_date_count,
            "null_transaction_timestamp_count": null_transaction_timestamp_count,
            "null_amount_count": null_amount_count,
            "null_currency_count": null_currency_count,
            "null_dq_flag_count": null_dq_flag_count,
            "implementation_ref": (
                "benchmarks.workloads.silver."
                "slv_03_transactions_standardise_pyarrow:run"
            ),
        },
    )
