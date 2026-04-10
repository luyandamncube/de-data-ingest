"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path

from deltalake import DeltaTable
import polars as pl

from pipeline.config_loader import load_config


AS_OF_DATE = date(2025, 12, 31)


def _active_file_uris(delta_table: DeltaTable, table_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(table_path / relative_path) for relative_path in delta_table.files()]


def _scan_delta_table(table_path: Path):
    delta_table = DeltaTable(str(table_path))
    files = _active_file_uris(delta_table, table_path)
    scan = pl.scan_parquet(files)
    schema_names = set(scan.collect_schema().names())
    return scan, schema_names


def _age_band_expr():
    dob = pl.col("dob").cast(pl.Date)
    age_years = pl.lit(AS_OF_DATE.year) - dob.dt.year()

    return (
        pl.when(age_years < 25)
        .then(pl.lit("18_24"))
        .when(age_years < 35)
        .then(pl.lit("25_34"))
        .when(age_years < 45)
        .then(pl.lit("35_44"))
        .when(age_years < 55)
        .then(pl.lit("45_54"))
        .when(age_years < 65)
        .then(pl.lit("55_64"))
        .otherwise(pl.lit("65_PLUS"))
    )


def _build_dim_customers(customers_path: str) -> pl.DataFrame:
    customers_scan, _ = _scan_delta_table(Path(customers_path))

    return (
        customers_scan
        .with_columns(_age_band_expr().alias("age_band"))
        .select(
            [
                pl.col("customer_id").cast(pl.String),
                pl.col("province").cast(pl.String),
                pl.col("segment").cast(pl.String),
                pl.col("income_band").cast(pl.String),
                pl.col("risk_score").cast(pl.Int32, strict=False),
                pl.col("kyc_status").cast(pl.String),
                pl.col("product_flags").cast(pl.String),
                pl.col("age_band").cast(pl.String),
            ]
        )
        .sort("customer_id")
        .collect()
        .with_row_index("customer_sk", offset=1)
        .with_columns(pl.col("customer_sk").cast(pl.Int64))
        .select(
            [
                "customer_sk",
                "customer_id",
                "province",
                "segment",
                "income_band",
                "risk_score",
                "kyc_status",
                "product_flags",
                "age_band",
            ]
        )
    )


def _build_dim_accounts(accounts_path: str) -> pl.DataFrame:
    accounts_scan, _ = _scan_delta_table(Path(accounts_path))

    return (
        accounts_scan
        .select(
            [
                pl.col("account_id").cast(pl.String),
                pl.col("customer_ref").cast(pl.String).alias("customer_id"),
                pl.col("account_type").cast(pl.String),
                pl.col("account_status").cast(pl.String),
                pl.col("open_date").cast(pl.Date),
                pl.col("product_tier").cast(pl.String),
                pl.col("mobile_number").cast(pl.String),
                pl.col("digital_channel").cast(pl.String),
                pl.col("credit_limit").cast(pl.Decimal(18, 2), strict=False),
                pl.col("current_balance").cast(pl.Decimal(18, 2), strict=False),
                pl.col("last_activity_date").cast(pl.Date),
            ]
        )
        .sort("account_id")
        .collect()
        .with_row_index("account_sk", offset=1)
        .with_columns(pl.col("account_sk").cast(pl.Int64))
        .select(
            [
                "account_sk",
                "account_id",
                "customer_id",
                "account_type",
                "account_status",
                "open_date",
                "product_tier",
                "mobile_number",
                "digital_channel",
                "credit_limit",
                "current_balance",
            ]
        )
    )


def _build_fact_transactions(
    transactions_path: str,
    dim_accounts: pl.DataFrame,
    dim_customers: pl.DataFrame,
) -> pl.DataFrame:
    transactions_scan, schema_names = _scan_delta_table(Path(transactions_path))

    merchant_subcategory_expr = (
        pl.col("merchant_subcategory").cast(pl.String)
        if "merchant_subcategory" in schema_names
        else pl.lit(None, dtype=pl.String)
    ).alias("merchant_subcategory")

    province_expr = (
        pl.col("province").cast(pl.String)
        if "province" in schema_names
        else pl.lit(None, dtype=pl.String)
    ).alias("province")

    city_expr = (
        pl.col("city").cast(pl.String)
        if "city" in schema_names
        else pl.lit(None, dtype=pl.String)
    ).alias("city")

    retry_flag_expr = (
        pl.col("retry_flag").cast(pl.Boolean)
        if "retry_flag" in schema_names
        else pl.lit(None, dtype=pl.Boolean)
    ).alias("retry_flag")

    account_map = dim_accounts.select(["account_id", "account_sk", "customer_id"]).lazy()
    customer_map = dim_customers.select(["customer_id", "customer_sk"]).lazy()

    return (
        transactions_scan
        .select(
            [
                pl.col("transaction_id").cast(pl.String),
                pl.col("account_id").cast(pl.String),
                pl.col("transaction_timestamp").cast(pl.Datetime("us")),
                pl.col("transaction_type").cast(pl.String),
                pl.col("merchant_category").cast(pl.String),
                merchant_subcategory_expr,
                pl.col("amount").cast(pl.Decimal(18, 2), strict=False),
                pl.col("currency").cast(pl.String),
                pl.col("channel").cast(pl.String),
                province_expr,
                city_expr,
                retry_flag_expr,
                pl.col("dq_flag").cast(pl.String),
            ]
        )
        .join(account_map, on="account_id", how="left")
        .join(customer_map, on="customer_id", how="left")
        .sort("transaction_id")
        .collect()
        .with_row_index("transaction_sk", offset=1)
        .with_columns(pl.col("transaction_sk").cast(pl.Int64))
        .select(
            [
                "transaction_sk",
                "transaction_id",
                "account_sk",
                "customer_sk",
                "transaction_timestamp",
                "transaction_type",
                "merchant_category",
                "merchant_subcategory",
                "amount",
                "currency",
                "channel",
                "province",
                "city",
                "retry_flag",
                "dq_flag",
            ]
        )
    )


def _write_delta_table(df: pl.DataFrame, output_path: str) -> None:
    output_table_path = Path(output_path)
    output_table_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_delta(str(output_table_path), mode="overwrite")


def _write_dq_report_if_configured(
    *,
    fact_transactions: pl.DataFrame,
    dq_report_path: str | None,
) -> None:
    if not dq_report_path:
        return

    total_records = fact_transactions.height
    clean_records = fact_transactions.filter(pl.col("dq_flag").is_null()).height
    flagged_records = total_records - clean_records

    flag_counts_df = (
        fact_transactions
        .filter(pl.col("dq_flag").is_not_null())
        .group_by("dq_flag")
        .len()
        .sort("dq_flag")
    )

    flag_counts = {
        row["dq_flag"]: int(row["len"])
        for row in flag_counts_df.to_dicts()
    }

    payload = {
        "total_records": int(total_records),
        "clean_records": int(clean_records),
        "flagged_records": int(flagged_records),
        "flag_counts": flag_counts,
    }

    report_path = Path(dq_report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def run_provisioning():
    config = load_config()
    Path(config.output.gold_path).mkdir(parents=True, exist_ok=True)

    dim_customers = _build_dim_customers(config.silver_table_path("customers"))
    dim_accounts = _build_dim_accounts(config.silver_table_path("accounts"))
    fact_transactions = _build_fact_transactions(
        config.silver_table_path("transactions"),
        dim_accounts,
        dim_customers,
    )

    _write_delta_table(dim_customers, config.gold_table_path("dim_customers"))
    _write_delta_table(dim_accounts, config.gold_table_path("dim_accounts"))
    _write_delta_table(fact_transactions, config.gold_table_path("fact_transactions"))

    _write_dq_report_if_configured(
        fact_transactions=fact_transactions,
        dq_report_path=config.output.dq_report_path,
    )