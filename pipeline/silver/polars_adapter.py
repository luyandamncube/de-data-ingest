"""Polars-backed Silver transform adapter."""

from __future__ import annotations

from pathlib import Path

from deltalake import DeltaTable

from pipeline.silver.base import SilverTransformAdapter


def _active_file_uris(delta_table: DeltaTable, table_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(table_path / relative_path) for relative_path in delta_table.files()]


def _scan_delta_table(table_path: Path):
    import polars as pl

    delta_table = DeltaTable(str(table_path))
    data_files = _active_file_uris(delta_table, table_path)
    scan = pl.scan_parquet(data_files)
    schema_names = set(scan.collect_schema().names())
    return scan, schema_names


def _parse_mixed_date_expr(pl, column_name: str):
    raw = pl.col(column_name).cast(pl.String).str.strip_chars()

    epoch_date = (
        pl.when(raw.str.len_bytes() >= 13)
        .then(pl.from_epoch(raw.cast(pl.Int64, strict=False), time_unit="ms").dt.date())
        .otherwise(
            pl.from_epoch(raw.cast(pl.Int64, strict=False), time_unit="s").dt.date()
        )
    )

    return (
        pl.when(raw.is_null() | (raw == ""))
        .then(pl.lit(None, dtype=pl.Date))
        .when(raw.str.contains(r"^\d{4}-\d{2}-\d{2}$"))
        .then(raw.str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        .when(raw.str.contains(r"^\d{2}/\d{2}/\d{4}$"))
        .then(raw.str.strptime(pl.Date, "%d/%m/%Y", strict=False))
        .when(raw.str.contains(r"^\d{10,13}$"))
        .then(epoch_date)
        .otherwise(pl.lit(None, dtype=pl.Date))
    )


def _canonical_currency_expr(pl):
    raw = pl.col("currency").cast(pl.String).str.strip_chars()
    lower = raw.str.to_lowercase()

    return (
        pl.when(lower.is_in(["zar", "r", "rands", "710"]))
        .then(pl.lit("ZAR"))
        .otherwise(raw.str.to_uppercase())
    )


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

        bronze_scan, _ = _scan_delta_table(input_table_path)
        customers_df = (
            bronze_scan
            .select(
                [
                    pl.col("customer_id").cast(pl.String),
                    pl.col("id_number").cast(pl.String),
                    pl.col("first_name").cast(pl.String),
                    pl.col("last_name").cast(pl.String),
                    _parse_mixed_date_expr(pl, "dob").alias("dob"),
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
            .sort(
                ["customer_id", "ingestion_timestamp"],
                descending=[False, True],
            )
            .unique(
                subset=["customer_id"],
                keep="first",
                maintain_order=True,
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

        bronze_scan, _ = _scan_delta_table(input_table_path)
        accounts_df = (
            bronze_scan
            .select(
                [
                    pl.col("account_id").cast(pl.String),
                    pl.col("customer_ref").cast(pl.String),
                    pl.col("account_type").cast(pl.String),
                    pl.col("account_status").cast(pl.String),
                    _parse_mixed_date_expr(pl, "open_date").alias("open_date"),
                    pl.col("product_tier").cast(pl.String),
                    pl.col("mobile_number").cast(pl.String),
                    pl.col("digital_channel").cast(pl.String),
                    pl.col("credit_limit").cast(pl.Decimal(18, 2), strict=False),
                    pl.col("current_balance").cast(pl.Decimal(18, 2), strict=False),
                    _parse_mixed_date_expr(pl, "last_activity_date").alias(
                        "last_activity_date"
                    ),
                    pl.col("ingestion_timestamp").cast(pl.Datetime("us")),
                ]
            )
            .sort(
                ["account_id", "ingestion_timestamp"],
                descending=[False, True],
            )
            .unique(
                subset=["account_id"],
                keep="first",
                maintain_order=True,
            )
            .collect()
        )
        accounts_df.write_delta(str(output_table_path), mode="overwrite")

    def transform_transactions(
        self,
        *,
        input_path: str,
        output_path: str,
        accounts_input_path: str | None = None,
    ) -> None:
        import polars as pl

        input_table_path = Path(input_path)
        output_table_path = Path(output_path)
        output_table_path.parent.mkdir(parents=True, exist_ok=True)

        bronze_scan, schema_names = _scan_delta_table(input_table_path)

        merchant_subcategory_expr = (
            pl.col("merchant_subcategory").cast(pl.String)
            if "merchant_subcategory" in schema_names
            else pl.lit(None, dtype=pl.String)
        ).alias("merchant_subcategory")

        province_expr = (
            pl.col("location").struct.field("province").cast(pl.String)
            if "location" in schema_names
            else pl.lit(None, dtype=pl.String)
        ).alias("province")

        city_expr = (
            pl.col("location").struct.field("city").cast(pl.String)
            if "location" in schema_names
            else pl.lit(None, dtype=pl.String)
        ).alias("city")

        coordinates_expr = (
            pl.col("location").struct.field("coordinates").cast(pl.String)
            if "location" in schema_names
            else pl.lit(None, dtype=pl.String)
        ).alias("coordinates")

        device_id_expr = (
            pl.col("metadata").struct.field("device_id").cast(pl.String)
            if "metadata" in schema_names
            else pl.lit(None, dtype=pl.String)
        ).alias("device_id")

        session_id_expr = (
            pl.col("metadata").struct.field("session_id").cast(pl.String)
            if "metadata" in schema_names
            else pl.lit(None, dtype=pl.String)
        ).alias("session_id")

        retry_flag_expr = (
            pl.col("metadata").struct.field("retry_flag").cast(pl.Boolean)
            if "metadata" in schema_names
            else pl.lit(None, dtype=pl.Boolean)
        ).alias("retry_flag")

        transaction_date_expr = _parse_mixed_date_expr(pl, "transaction_date").alias(
            "transaction_date"
        )

        transaction_timestamp_expr = (
            pl.concat_str(
                [
                    pl.col("transaction_date").cast(pl.String),
                    pl.lit(" "),
                    pl.col("transaction_time").cast(pl.String),
                ]
            )
            .str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S", strict=False)
            .alias("transaction_timestamp")
        )

        transactions_lf = (
            bronze_scan
            .with_columns(
                [
                    pl.col("transaction_date").cast(pl.String).alias(
                        "_raw_transaction_date"
                    ),
                    pl.col("currency").cast(pl.String).alias("_raw_currency"),
                    (pl.len().over("transaction_id") > 1).alias("_is_duplicate_group"),
                ]
            )
            .select(
                [
                    pl.col("transaction_id").cast(pl.String),
                    pl.col("account_id").cast(pl.String),
                    transaction_date_expr,
                    pl.col("transaction_time").cast(pl.String),
                    transaction_timestamp_expr,
                    pl.col("transaction_type").cast(pl.String),
                    pl.col("merchant_category").cast(pl.String),
                    merchant_subcategory_expr,
                    pl.col("amount").cast(pl.Decimal(18, 2), strict=False),
                    _canonical_currency_expr(pl).alias("currency"),
                    pl.col("channel").cast(pl.String),
                    province_expr,
                    city_expr,
                    coordinates_expr,
                    device_id_expr,
                    session_id_expr,
                    retry_flag_expr,
                    pl.col("ingestion_timestamp").cast(pl.Datetime("us")),
                    pl.col("_raw_transaction_date"),
                    pl.col("_raw_currency"),
                    pl.col("_is_duplicate_group"),
                ]
            )
            .sort(
                ["transaction_id", "ingestion_timestamp"],
                descending=[False, True],
            )
            .unique(
                subset=["transaction_id"],
                keep="first",
                maintain_order=True,
            )
        )

        if accounts_input_path is not None:
            accounts_scan, _ = _scan_delta_table(Path(accounts_input_path))
            valid_accounts_lf = (
                accounts_scan
                .select(
                    [
                        pl.col("account_id").cast(pl.String),
                        pl.lit(True).alias("_has_account_match"),
                    ]
                )
                .unique(subset=["account_id"], keep="first", maintain_order=True)
            )
            transactions_lf = transactions_lf.join(
                valid_accounts_lf,
                on="account_id",
                how="left",
            )
        else:
            transactions_lf = transactions_lf.with_columns(
                pl.lit(True).alias("_has_account_match")
            )

        transactions_df = (
            transactions_lf
            .with_columns(
                [
                    (
                        pl.col("_raw_currency")
                        .cast(pl.String)
                        .str.strip_chars()
                        .str.to_lowercase()
                        .is_in(["r", "rands", "zar", "710"])
                        & (
                            pl.col("_raw_currency")
                            .cast(pl.String)
                            .str.strip_chars()
                            != "ZAR"
                        )
                    ).alias("_is_currency_variant"),
                    (
                        pl.col("_raw_transaction_date").is_not_null()
                        & (
                            pl.col("_raw_transaction_date")
                            .cast(pl.String)
                            .str.strip_chars()
                            != ""
                        )
                        & pl.col("transaction_date").is_null()
                    ).alias("_has_date_format_issue"),
                ]
            )
            .with_columns(
                pl.when(pl.col("_has_account_match").fill_null(False).not_())
                .then(pl.lit("ORPHANED_ACCOUNT"))
                .when(pl.col("_is_duplicate_group"))
                .then(pl.lit("DUPLICATE_DEDUPED"))
                .when(pl.col("_has_date_format_issue"))
                .then(pl.lit("DATE_FORMAT"))
                .when(pl.col("_is_currency_variant"))
                .then(pl.lit("CURRENCY_VARIANT"))
                .otherwise(pl.lit(None, dtype=pl.String))
                .alias("dq_flag")
            )
            .drop(
                "_raw_transaction_date",
                "_raw_currency",
                "_is_duplicate_group",
                "_has_account_match",
                "_is_currency_variant",
                "_has_date_format_issue",
            )
            .collect()
        )

        transactions_df.write_delta(str(output_table_path), mode="overwrite")