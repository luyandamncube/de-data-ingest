"""Spark session helpers for the batch pipeline."""

from __future__ import annotations

import os
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from pipeline.config_loader import SparkConfig


def build_spark_session(config: SparkConfig) -> SparkSession:
    """Create a Delta-enabled local Spark session"""

    native_tmp_dir = _prepare_spark_environment()
    java_tmp_options = (
        f"-Djava.io.tmpdir={native_tmp_dir} "
        f"-Dorg.xerial.snappy.tempdir={native_tmp_dir}"
    )

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
        .config("spark.driver.extraJavaOptions", java_tmp_options)
        .config("spark.executor.extraJavaOptions", java_tmp_options)
    )
    if config.local_dir:
        builder = builder.config("spark.local.dir", config.local_dir)

    return builder.getOrCreate()


def _prepare_spark_environment() -> str:
    spark_home = Path(pyspark.__file__).resolve().parent
    spark_bin = str(spark_home / "bin")
    ivy_home = "/tmp/.ivy2"
    native_tmp_dir = "/data/output/.spark-native"

    os.environ["SPARK_HOME"] = str(spark_home)
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    os.environ["HOME"] = os.environ.get("HOME") or "/home/pipeline"
    os.environ["USER"] = os.environ.get("USER") or "pipeline"
    os.environ["LOGNAME"] = os.environ.get("LOGNAME") or os.environ["USER"]
    os.environ["HADOOP_USER_NAME"] = (
        os.environ.get("HADOOP_USER_NAME") or os.environ["USER"]
    )
    os.environ["IVY_HOME"] = ivy_home

    current_path = os.environ.get("PATH", "")
    path_entries = current_path.split(":") if current_path else []
    if spark_bin not in path_entries:
        os.environ["PATH"] = f"{spark_bin}:{current_path}" if current_path else spark_bin

    Path(ivy_home).mkdir(parents=True, exist_ok=True)
    Path(native_tmp_dir).mkdir(parents=True, exist_ok=True)
    return native_tmp_dir
