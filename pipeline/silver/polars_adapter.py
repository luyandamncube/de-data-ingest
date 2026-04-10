"""Polars-backed Silver transform adapter."""

from __future__ import annotations

from pathlib import Path

from deltalake import DeltaTable

from pipeline.silver.base import SilverTransformAdapter


def _active_file_uris(delta_table: DeltaTable, table_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(table_path / relative_path) for relative_path in delta_table.files()]


class PolarsSilverAdapter(SilverTransformAdapter):
    engine_name = "polars"

    def transform_customers(
        self,
        *,
        input_path: str,
        output_path: str,
    ) -> None:
        import polars as pl

        input_table_path = Path(input_path)
        output_table_path = Path(output_path)
        output_table_path.parent.mkdir(parents=True, exist_ok=True)

        bronze_table = DeltaTable(str(input_table_path))
        bronze_files = _active_file_uris(bronze_table, input_table_path)
        customers_df = (
            pl.scan_parquet(bronze_files)
            .select(
                [
                    pl.col("customer_id").cast(pl.String),
                    pl.col("id_number").cast(pl.String),
                    pl.col("first_name").cast(pl.String),
                    pl.col("last_name").cast(pl.String),
                    pl.col("dob").cast(pl.String).str.strptime(
                        pl.Date,
                        "%Y-%m-%d",
                        strict=False,
                    ),
                    pl.col("gender").cast(pl.String),
                    pl.col("province").cast(pl.String),
                    pl.col("income_band").cast(pl.String),
                    pl.col("segment").cast(pl.String),
                    pl.col("risk_score").cast(pl.Int32, strict=False),
                    pl.col("kyc_status").cast(pl.String),
                    pl.col("product_flags").cast(pl.String),
                    pl.col("ingestion_timestamp").cast(pl.Datetime("us")),
                ]
            )
            .collect()
        )
        customers_df.write_delta(str(output_table_path), mode="overwrite")

    def transform_accounts(
        self,
        *,
        input_path: str,
        output_path: str,
    ) -> None:
        import polars as pl

        input_table_path = Path(input_path)
        output_table_path = Path(output_path)
        output_table_path.parent.mkdir(parents=True, exist_ok=True)

        bronze_table = DeltaTable(str(input_table_path))
        bronze_files = _active_file_uris(bronze_table, input_table_path)
        accounts_df = (
            pl.scan_parquet(bronze_files)
            .select(
                [
                    pl.col("account_id").cast(pl.String),
                    pl.col("customer_ref").cast(pl.String),
                    pl.col("account_type").cast(pl.String),
                    pl.col("account_status").cast(pl.String),
                    pl.col("open_date").cast(pl.String).str.strptime(
                        pl.Date,
                        "%Y-%m-%d",
                        strict=False,
                    ),
                    pl.col("product_tier").cast(pl.String),
                    pl.col("mobile_number").cast(pl.String),
                    pl.col("digital_channel").cast(pl.String),
                    pl.col("credit_limit").cast(pl.Decimal(18, 2), strict=False),
                    pl.col("current_balance").cast(pl.Decimal(18, 2), strict=False),
                    pl.col("last_activity_date").cast(pl.String).str.strptime(
                        pl.Date,
                        "%Y-%m-%d",
                        strict=False,
                    ),
                    pl.col("ingestion_timestamp").cast(pl.Datetime("us")),
                ]
            )
            .collect()
        )
        accounts_df.write_delta(str(output_table_path), mode="overwrite")
