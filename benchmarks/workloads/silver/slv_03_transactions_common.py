"""Shared helpers for SLV_03 transaction Silver benchmark workloads."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path


def count_jsonl_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def active_file_uris(delta_table, table_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(table_path / relative_path) for relative_path in delta_table.files()]


def source_schema():
    import pyarrow as pa

    return pa.schema(
        [
            ("transaction_id", pa.string()),
            ("account_id", pa.string()),
            ("transaction_date", pa.string()),
            ("transaction_time", pa.string()),
            ("transaction_type", pa.string()),
            ("merchant_category", pa.string()),
            ("merchant_subcategory", pa.string()),
            ("amount", pa.float64()),
            ("currency", pa.string()),
            ("channel", pa.string()),
            (
                "location",
                pa.struct(
                    [
                        ("province", pa.string()),
                        ("city", pa.string()),
                        ("coordinates", pa.string()),
                    ]
                ),
            ),
            (
                "metadata",
                pa.struct(
                    [
                        ("device_id", pa.string()),
                        ("session_id", pa.string()),
                        ("retry_flag", pa.bool_()),
                    ]
                ),
            ),
        ]
    )


def build_bronze_transactions_table(transactions_path: Path, run_timestamp: datetime):
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.json as paj

    source_table = paj.read_json(
        transactions_path,
        read_options=paj.ReadOptions(use_threads=True),
        parse_options=paj.ParseOptions(explicit_schema=source_schema()),
    )
    amount = pc.cast(
        source_table.column("amount"),
        pa.decimal128(18, 2),
        safe=False,
    )
    ingestion_timestamp = pa.array(
        [run_timestamp] * source_table.num_rows,
        type=pa.timestamp("us"),
    )
    return pa.table(
        {
            "transaction_id": source_table.column("transaction_id"),
            "account_id": source_table.column("account_id"),
            "transaction_date": source_table.column("transaction_date"),
            "transaction_time": source_table.column("transaction_time"),
            "transaction_type": source_table.column("transaction_type"),
            "merchant_category": source_table.column("merchant_category"),
            "merchant_subcategory": source_table.column("merchant_subcategory"),
            "amount": amount,
            "currency": source_table.column("currency"),
            "channel": source_table.column("channel"),
            "location": source_table.column("location"),
            "metadata": source_table.column("metadata"),
            "ingestion_timestamp": ingestion_timestamp,
        }
    )
