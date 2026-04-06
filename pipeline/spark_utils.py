"""Spark session helpers for the batch pipeline."""

from __future__ import annotations

import os
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from pipeline.config_loader import SparkConfig


def build_spark_session(config: SparkConfig) -> SparkSession:
    """Create a Delta-enabled local Spark session"""

    _prepare_spark_environment()

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
        .config("spark.jars.ivy", "/tmp/.ivy2")
    )
    if config.local_dir:
        builder = builder.config("spark.local.dir", config.local_dir)

    return builder.getOrCreate()


def _prepare_spark_environment() -> None:
    spark_home = Path(pyspark.__file__).resolve().parent
    spark_bin = str(spark_home / "bin")
    ivy_home = "/tmp/.ivy2"

    os.environ["SPARK_HOME"] = str(spark_home)
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    os.environ.setdefault("HOME", "/tmp")
    os.environ.setdefault("USER", "coder")
    os.environ["IVY_HOME"] = ivy_home

    current_path = os.environ.get("PATH", "")
    path_entries = current_path.split(":") if current_path else []
    if spark_bin not in path_entries:
        os.environ["PATH"] = f"{spark_bin}:{current_path}" if current_path else spark_bin

    Path(ivy_home).mkdir(parents=True, exist_ok=True)
