"""Polars-backed Bronze ingest adapter."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline.bronze.base import BronzeIngestAdapter
from pipeline.schemas import BRONZE_ACCOUNTS_SCHEMA
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA
from pyspark.sql import types as T


def _polars_dtype(data_type: T.DataType):
    import polars as pl

    if isinstance(data_type, T.StringType):
        return pl.String
    if isinstance(data_type, T.IntegerType):
        return pl.Int32
    if isinstance(data_type, T.DecimalType):
        return pl.Decimal(data_type.precision, data_type.scale)
    raise TypeError(f"unsupported Polars dtype mapping for {data_type}")


def _polars_schema(schema) -> dict[str, Any]:
    return {field.name: _polars_dtype(field.dataType) for field in schema.fields}


class PolarsBronzeAdapter(BronzeIngestAdapter):
    engine_name = "polars"

    def ingest_customers(
        self,
        *,
        input_path: str,
        output_path: str,
        run_timestamp: datetime,
    ) -> None:
        import polars as pl

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        customers_df = pl.read_csv(
            input_path,
            schema=_polars_schema(BRONZE_CUSTOMERS_SCHEMA),
        )
        customers_with_ts = customers_df.with_columns(
            pl.lit(run_timestamp).cast(pl.Datetime("us")).alias("ingestion_timestamp")
        )
        customers_with_ts.write_delta(output_path, mode="overwrite")

    def ingest_accounts(
        self,
        *,
        input_path: str,
        output_path: str,
        run_timestamp: datetime,
    ) -> None:
        import polars as pl

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        accounts_df = pl.read_csv(
            input_path,
            schema=_polars_schema(BRONZE_ACCOUNTS_SCHEMA),
        )
        accounts_with_ts = accounts_df.with_columns(
            pl.lit(run_timestamp).cast(pl.Datetime("us")).alias("ingestion_timestamp")
        )
        accounts_with_ts.write_delta(output_path, mode="overwrite")
