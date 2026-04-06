"""Factory helpers for Bronze ingest adapters."""

from __future__ import annotations

from pipeline.bronze.base import BronzeIngestAdapter
from pipeline.bronze.polars_adapter import PolarsBronzeAdapter
from pipeline.bronze.pyspark_adapter import PySparkBronzeAdapter
from pipeline.config_loader import SparkConfig


def build_bronze_adapter(
    engine_name: str,
    *,
    spark_config: SparkConfig,
) -> BronzeIngestAdapter:
    if engine_name == "polars":
        return PolarsBronzeAdapter()
    if engine_name == "pyspark_delta":
        return PySparkBronzeAdapter(spark_config)
    raise ValueError(f"unsupported Bronze engine: {engine_name}")
