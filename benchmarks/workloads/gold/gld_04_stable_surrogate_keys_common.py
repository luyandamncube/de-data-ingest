"""Shared helpers for GLD_04 stable surrogate key workloads."""

from __future__ import annotations

from pathlib import Path


def active_file_uris(delta_table, table_path: Path) -> list[str]:
    return list(delta_table.file_uris())


def build_stable_surrogate_key_fixtures(
    *,
    customers_path: Path,
    accounts_path: Path,
    customers_output_path: Path,
    accounts_output_path: Path,
) -> tuple[int, int, int]:
    import polars as pl

    customers_df = pl.read_csv(str(customers_path))
    accounts_df = pl.read_csv(str(accounts_path))

    customer_rows = customers_df.height
    account_rows = accounts_df.height
    rows_in = customer_rows + account_rows

    customers_output_path.mkdir(parents=True, exist_ok=True)
    accounts_output_path.mkdir(parents=True, exist_ok=True)

    customers_df.write_delta(str(customers_output_path), mode="overwrite")
    accounts_df.write_delta(str(accounts_output_path), mode="overwrite")

    return customer_rows, account_rows, rows_in