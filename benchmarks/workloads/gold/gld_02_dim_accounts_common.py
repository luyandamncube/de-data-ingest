"""Shared helpers for GLD_02 dim_accounts workloads."""

from __future__ import annotations

from pathlib import Path


def active_file_uris(delta_table, table_path: Path) -> list[str]:
    return list(delta_table.file_uris())


def build_dim_accounts_fixture(
    *,
    accounts_path: Path,
    output_path: Path,
) -> tuple[int, int]:
    import polars as pl

    accounts_df = pl.read_csv(str(accounts_path))
    base_rows = accounts_df.height

    output_path.mkdir(parents=True, exist_ok=True)
    accounts_df.write_delta(str(output_path), mode="overwrite")

    rows_in = base_rows
    return base_rows, rows_in