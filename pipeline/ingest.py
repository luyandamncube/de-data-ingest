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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.config_loader import load_config
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA
from pipeline.spark_utils import build_spark_session


def run_ingestion() -> None:
    """Run the Bronze ingestion entrypoint."""

    config = load_config()
    spark = build_spark_session(config.spark)
    run_timestamp = _run_ingestion_timestamp()
    try:
        customers_df = _read_customers_raw(spark, config.input.customers_path)
        _write_bronze_table(
            df=_with_ingestion_timestamp(customers_df, run_timestamp),
            output_path=config.bronze_table_path("customers"),
        )

        # TODO: Implement BRZ_02 and BRZ_03 using the same run_timestamp:
        #   - accounts.csv -> bronze/accounts
        #   - transactions.jsonl -> bronze/transactions
    finally:
        spark.stop()


def _run_ingestion_timestamp() -> datetime:
    """Return one UTC timestamp shared across the Bronze ingestion run."""

    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)


def _read_customers_raw(spark, input_path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .schema(BRONZE_CUSTOMERS_SCHEMA)
        .csv(input_path)
    )


def _with_ingestion_timestamp(df: DataFrame, run_timestamp: datetime) -> DataFrame:
    return df.withColumn("ingestion_timestamp", F.lit(run_timestamp).cast("timestamp"))


def _write_bronze_table(*, df: DataFrame, output_path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )
