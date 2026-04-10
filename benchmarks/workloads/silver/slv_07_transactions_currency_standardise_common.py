"""Shared helpers for SLV_07 transactions currency standardisation workloads."""

from __future__ import annotations

from pathlib import Path

CURRENCY_VARIANT_FRACTION = 0.10
CANONICAL_CURRENCY = "ZAR"
INJECTED_VARIANTS = ("R", "rands", "zar", "710")


def active_file_uris(delta_table, table_path: Path) -> list[str]:
    return [str(table_path / rel_path) for rel_path in delta_table.file_uris()]


def build_currency_variants_fixture(
    *,
    transactions_path: Path,
    output_path: Path,
) -> tuple[int, int, int]:
    import polars as pl

    transactions_df = pl.read_ndjson(str(transactions_path))
    base_rows = transactions_df.height
    variant_rows = max(1, int(base_rows * CURRENCY_VARIANT_FRACTION))

    variant_expr = (
        pl.when(pl.col("_row_nr") < variant_rows)
        .then(
            pl.when((pl.col("_row_nr") % 4) == 0)
            .then(pl.lit("R"))
            .when((pl.col("_row_nr") % 4) == 1)
            .then(pl.lit("rands"))
            .when((pl.col("_row_nr") % 4) == 2)
            .then(pl.lit("zar"))
            .otherwise(pl.lit("710"))
        )
        .otherwise(pl.col("currency").cast(pl.Utf8))
    )

    fixture_df = (
        transactions_df
        .with_row_index("_row_nr")
        .with_columns(variant_expr.alias("currency"))
        .drop("_row_nr")
    )

    rows_in = fixture_df.height
    output_path.mkdir(parents=True, exist_ok=True)
    fixture_df.write_delta(str(output_path), mode="overwrite")

    return base_rows, variant_rows, rows_in