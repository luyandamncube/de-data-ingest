"""Shared helpers for SLV_04 customer dedup benchmark workloads."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA


DEDUP_FRACTION = 0.10


def count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def active_file_uris(delta_table, table_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(table_path / relative_path) for relative_path in delta_table.files()]


def _bronze_polars_schema() -> dict[str, object]:
    import polars as pl

    return {field.name: pl.String for field in BRONZE_CUSTOMERS_SCHEMA.fields}


def build_duplicate_customers_fixture(
    *,
    customers_path: Path,
    output_path: Path,
    run_timestamp: datetime,
) -> tuple[int, int, int]:
    import polars as pl

    base_rows = count_csv_rows(customers_path)
    duplicate_rows = int(base_rows * DEDUP_FRACTION)
    base_df = (
        pl.read_csv(
            customers_path,
            schema=_bronze_polars_schema(),
        )
        .with_row_index("source_row_nr")
        .with_columns(
            [
                pl.col("dob").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                pl.col("risk_score").cast(pl.Int32, strict=False),
                pl.lit(run_timestamp).cast(pl.Datetime("us")).alias(
                    "ingestion_timestamp"
                ),
            ]
        )
    )
    duplicate_df = base_df.head(duplicate_rows).with_columns(
        (
            pl.col("ingestion_timestamp") + timedelta(seconds=1)
        ).alias("ingestion_timestamp")
    )
    fixture_df = pl.concat([base_df, duplicate_df], how="vertical").drop("source_row_nr")
    fixture_df.write_delta(str(output_path), mode="overwrite")
    return base_rows, duplicate_rows, fixture_df.height
