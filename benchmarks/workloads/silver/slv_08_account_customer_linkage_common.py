"""Shared helpers for SLV_08 account-customer linkage workloads."""

from __future__ import annotations

from pathlib import Path


def active_file_uris(delta_table, table_path: Path) -> list[str]:
    # Safer: let delta-rs provide the active file locations directly.
    return list(delta_table.file_uris())


def build_account_customer_linkage_fixture(
    *,
    accounts_path: Path,
    customers_path: Path,
    accounts_output_path: Path,
    customers_output_path: Path,
) -> tuple[int, int, int]:
    import polars as pl

    accounts_df = pl.read_csv(str(accounts_path))
    customers_df = pl.read_csv(str(customers_path))

    account_rows = accounts_df.height
    customer_rows = customers_df.height
    rows_in = account_rows + customer_rows

    accounts_output_path.mkdir(parents=True, exist_ok=True)
    customers_output_path.mkdir(parents=True, exist_ok=True)

    accounts_df.write_delta(str(accounts_output_path), mode="overwrite")
    customers_df.write_delta(str(customers_output_path), mode="overwrite")

    return account_rows, customer_rows, rows_in