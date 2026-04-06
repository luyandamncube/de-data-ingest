"""Spark session helpers for the batch pipeline."""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.config_loader import SparkConfig


def build_spark_session(config: SparkConfig) -> SparkSession:
    """Create a Delta-enabled local Spark session"""

    builder = (
        SparkSession.builder.appName(config.app_name)
        .master(config.master)
        .config("spark.ui.enabled", "false")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
    )
    if config.local_dir:
        builder = builder.config("spark.local.dir", config.local_dir)

    return builder.getOrCreate()
