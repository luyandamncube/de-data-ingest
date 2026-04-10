"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Input paths (Bronze layer output — read these, do not modify):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Output paths (your pipeline must create these directories):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Requirements:
  - Deduplicate records within each table on natural keys
    (account_id, transaction_id, customer_id respectively).
  - Standardise data types (e.g. parse date strings to DATE, cast amounts to
    DECIMAL(18,2), normalise currency variants to "ZAR").
  - Apply DQ flagging to transactions:
      - Set dq_flag = NULL for clean records.
      - Set dq_flag to the appropriate issue code for flagged records.
      - Valid codes: ORPHANED_ACCOUNT, DUPLICATE_DEDUPED, TYPE_MISMATCH,
        DATE_FORMAT, CURRENCY_VARIANT, NULL_REQUIRED.
  - At Stage 2, load DQ rules from config/dq_rules.yaml rather than hardcoding.
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.

See output_schema_spec.md §8 for the full list of DQ flag values and their
definitions.
"""

from pathlib import Path

from pipeline.config_loader import load_config
from pipeline.silver.factory import build_silver_adapter


def run_transformation():
    config = load_config()
    Path(config.output.silver_path).mkdir(parents=True, exist_ok=True)
    adapters = {}

    def get_adapter(engine_name: str):
        adapter = adapters.get(engine_name)
        if adapter is None:
            adapter = build_silver_adapter(engine_name)
            adapters[engine_name] = adapter
        return adapter

    try:
        customers_adapter = get_adapter(config.silver.engines.customers)
        customers_adapter.transform_customers(
            input_path=config.bronze_table_path("customers"),
            output_path=config.silver_table_path("customers"),
        )

        # TODO: Implement SLV_02 / SLV_03 / SLV_04+ using the same adapter shape.
    finally:
        for adapter in adapters.values():
            adapter.close()
