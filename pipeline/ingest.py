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

from pipeline.bronze.factory import build_bronze_adapter
from pipeline.config_loader import load_config


def run_ingestion() -> None:
    """Run the Bronze ingestion entrypoint."""

    config = load_config()
    Path(config.output.bronze_path).mkdir(parents=True, exist_ok=True)
    run_timestamp = _run_ingestion_timestamp()
    adapters = {}

    def get_adapter(engine_name: str):
        adapter = adapters.get(engine_name)
        if adapter is None:
            adapter = build_bronze_adapter(
                engine_name,
                spark_config=config.spark,
            )
            adapters[engine_name] = adapter
        return adapter

    try:
        customers_adapter = get_adapter(config.ingest.engines.customers)
        customers_adapter.ingest_customers(
            input_path=config.input.customers_path,
            output_path=config.bronze_table_path("customers"),
            run_timestamp=run_timestamp,
        )

        accounts_adapter = get_adapter(config.ingest.engines.accounts)
        accounts_adapter.ingest_accounts(
            input_path=config.input.accounts_path,
            output_path=config.bronze_table_path("accounts"),
            run_timestamp=run_timestamp,
        )

        # TODO: Implement BRZ_03 using the same run_timestamp:
        #   - transactions.jsonl -> bronze/transactions
    finally:
        for adapter in adapters.values():
            adapter.close()


def _run_ingestion_timestamp() -> datetime:
    """Return one UTC timestamp shared across the Bronze ingestion run."""

    return datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
