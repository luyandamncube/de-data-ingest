"""Shared helpers for SLV_06 transactions Silver dedup workloads."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

DEDUP_FRACTION = 0.10


def active_file_uris(delta_table, table_path: Path) -> list[str]:
    return [str(table_path / rel_path) for rel_path in delta_table.file_uris()]


def build_duplicate_transactions_fixture(
    *,
    transactions_path: Path,
    output_path: Path,
    run_timestamp: datetime,
) -> tuple[int, int, int]:
    import polars as pl

    transactions_df = pl.read_ndjson(str(transactions_path))

    base_rows = transactions_df.height
    duplicate_count = max(1, int(base_rows * DEDUP_FRACTION))

    base_with_ts = transactions_df.with_columns(
        pl.lit(run_timestamp).alias("ingestion_timestamp")
    )

    duplicate_rows = (
        transactions_df.head(duplicate_count)
        .with_columns(
            pl.lit(run_timestamp + timedelta(minutes=1)).alias("ingestion_timestamp")
        )
    )

    fixture_df = pl.concat([base_with_ts, duplicate_rows], how="vertical")
    rows_in = fixture_df.height

    output_path.mkdir(parents=True, exist_ok=True)
    fixture_df.write_delta(str(output_path), mode="overwrite")

    return base_rows, duplicate_rows.height, rows_in