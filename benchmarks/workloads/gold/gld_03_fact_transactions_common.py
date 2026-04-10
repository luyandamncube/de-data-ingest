"""Shared helpers for GLD_03 fact_transactions workloads."""

from __future__ import annotations

from pathlib import Path


def active_file_uris(delta_table, table_path: Path) -> list[str]:
    return list(delta_table.file_uris())


def build_fact_transactions_fixtures(
    *,
    accounts_path: Path,
    customers_path: Path,
    transactions_path: Path,
    accounts_output_path: Path,
    customers_output_path: Path,
    transactions_output_path: Path,
) -> tuple[int, int, int, int]:
    import polars as pl

    accounts_df = pl.read_csv(str(accounts_path))
    customers_df = pl.read_csv(str(customers_path))
    transactions_df = pl.read_ndjson(str(transactions_path))

    if "merchant_subcategory" not in transactions_df.columns:
        transactions_df = transactions_df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("merchant_subcategory")
        )

    if "location" in transactions_df.columns:
        transactions_df = transactions_df.with_columns(
            pl.col("location").struct.field("province").alias("location_province"),
            pl.col("location").struct.field("city").alias("location_city"),
            pl.col("location").struct.field("coordinates").alias("location_coordinates"),
        )
    else:
        transactions_df = transactions_df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("location_province"),
            pl.lit(None, dtype=pl.Utf8).alias("location_city"),
            pl.lit(None, dtype=pl.Utf8).alias("location_coordinates"),
        )

    if "metadata" in transactions_df.columns:
        transactions_df = transactions_df.with_columns(
            pl.col("metadata").struct.field("device_id").alias("metadata_device_id"),
            pl.col("metadata").struct.field("session_id").alias("metadata_session_id"),
            pl.col("metadata").struct.field("retry_flag").alias("metadata_retry_flag"),
        )
    else:
        transactions_df = transactions_df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("metadata_device_id"),
            pl.lit(None, dtype=pl.Utf8).alias("metadata_session_id"),
            pl.lit(None, dtype=pl.Boolean).alias("metadata_retry_flag"),
        )

    drop_cols = [col for col in ("location", "metadata") if col in transactions_df.columns]
    if drop_cols:
        transactions_df = transactions_df.drop(drop_cols)

    account_rows = accounts_df.height
    customer_rows = customers_df.height
    transaction_rows = transactions_df.height
    rows_in = account_rows + customer_rows + transaction_rows

    accounts_output_path.mkdir(parents=True, exist_ok=True)
    customers_output_path.mkdir(parents=True, exist_ok=True)
    transactions_output_path.mkdir(parents=True, exist_ok=True)

    accounts_df.write_delta(str(accounts_output_path), mode="overwrite")
    customers_df.write_delta(str(customers_output_path), mode="overwrite")
    transactions_df.write_delta(str(transactions_output_path), mode="overwrite")

    return account_rows, customer_rows, transaction_rows, rows_in