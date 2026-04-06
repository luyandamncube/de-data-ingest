"""Polars-backed Bronze ingest adapter."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from pipeline.bronze.base import BronzeIngestAdapter
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA


def _polars_string_schema() -> dict[str, Any]:
    import polars as pl

    return {field.name: pl.String for field in BRONZE_CUSTOMERS_SCHEMA.fields}


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
            schema=_polars_string_schema(),
        )
        customers_with_ts = customers_df.with_columns(
            pl.lit(run_timestamp).cast(pl.Datetime("us")).alias("ingestion_timestamp")
        )
        customers_with_ts.write_delta(output_path, mode="overwrite")
