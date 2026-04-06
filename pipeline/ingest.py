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

from pipeline.config_loader import load_config
from pipeline.spark_utils import build_spark_session


def run_ingestion() -> None:
    """Run the Bronze ingestion entrypoint."""

    config = load_config()
    spark = build_spark_session(config.spark)
    try:
        # TODO: Implement Bronze layer ingestion.
        #
        # Suggested steps:
        #   1. Read the raw mounted inputs using explicit Bronze schemas.
        #   2. Add a single run-level ingestion_timestamp across all sources.
        #   3. Write accounts, transactions, and customers to Bronze Delta paths.
        _ = (config, spark)
    finally:
        spark.stop()
