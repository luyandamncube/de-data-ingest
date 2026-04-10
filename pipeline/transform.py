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
"""

from pathlib import Path

from pipeline.config_loader import load_config
from pipeline.silver.factory import build_silver_adapter


def _validate_account_customer_linkage(
    *,
    customers_path: str,
    accounts_path: str,
) -> None:
    from pathlib import Path

    from deltalake import DeltaTable
    import polars as pl

    def active_file_uris(delta_table: DeltaTable, table_path: Path) -> list[str]:
        if hasattr(delta_table, "file_uris"):
            return list(delta_table.file_uris())
        return [str(table_path / relative_path) for relative_path in delta_table.files()]

    customers_table_path = Path(customers_path)
    accounts_table_path = Path(accounts_path)

    customers_table = DeltaTable(str(customers_table_path))
    accounts_table = DeltaTable(str(accounts_table_path))

    customer_files = active_file_uris(customers_table, customers_table_path)
    account_files = active_file_uris(accounts_table, accounts_table_path)

    unmatched_count = (
        pl.scan_parquet(account_files)
        .select(pl.col("customer_ref").cast(pl.String).alias("customer_ref"))
        .join(
            pl.scan_parquet(customer_files)
            .select(pl.col("customer_id").cast(pl.String).alias("customer_id")),
            left_on="customer_ref",
            right_on="customer_id",
            how="anti",
        )
        .select(pl.len().alias("row_count"))
        .collect()
        .item()
    )

    if unmatched_count:
        raise ValueError(
            "silver linkage validation failed: "
            f"{unmatched_count} account rows do not match a customer_id"
        )


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

        accounts_adapter = get_adapter(config.silver.engines.accounts)
        accounts_adapter.transform_accounts(
            input_path=config.bronze_table_path("accounts"),
            output_path=config.silver_table_path("accounts"),
        )

        _validate_account_customer_linkage(
            customers_path=config.silver_table_path("customers"),
            accounts_path=config.silver_table_path("accounts"),
        )

        transactions_adapter = get_adapter(config.silver.engines.transactions)
        transaction_kwargs = {
            "input_path": config.bronze_table_path("transactions"),
            "output_path": config.silver_table_path("transactions"),
        }

        if getattr(transactions_adapter, "engine_name", None) == "polars":
            transaction_kwargs["accounts_input_path"] = config.silver_table_path(
                "accounts"
            )

        transactions_adapter.transform_transactions(**transaction_kwargs)
    finally:
        for adapter in adapters.values():
            adapter.close()