"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.config_loader import load_config
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA


def run_ingestion() -> None:
    """Run the Bronze ingestion entrypoint."""

    config = load_config()
    Path(config.output.bronze_path).mkdir(parents=True, exist_ok=True)
    run_timestamp = _run_ingestion_timestamp()
    customers_df = _read_customers_raw(config.input.customers_path)
    _write_bronze_table(
        df=_with_ingestion_timestamp(customers_df, run_timestamp),
        output_path=config.bronze_table_path("customers"),
    )

    # TODO: Implement BRZ_02 and BRZ_03 using the same run_timestamp:
    #   - accounts.csv -> bronze/accounts
    #   - transactions.jsonl -> bronze/transactions


def _run_ingestion_timestamp() -> datetime:
    """Return one UTC timestamp shared across the Bronze ingestion run."""

    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)


def _polars_string_schema() -> dict[str, Any]:
    import polars as pl

    return {field.name: pl.String for field in BRONZE_CUSTOMERS_SCHEMA.fields}


def _read_customers_raw(input_path: str):
    import polars as pl

    return pl.read_csv(
        input_path,
        schema=_polars_string_schema(),
    )


def _with_ingestion_timestamp(df, run_timestamp: datetime):
    import polars as pl

    return df.with_columns(
        pl.lit(run_timestamp).cast(pl.Datetime("us")).alias("ingestion_timestamp")
    )


def _write_bronze_table(*, df, output_path: str) -> None:
    df.write_delta(output_path, mode="overwrite")
