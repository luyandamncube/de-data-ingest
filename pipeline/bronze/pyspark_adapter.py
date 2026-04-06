"""PySpark-backed Bronze ingest adapter."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pipeline.bronze.base import BronzeIngestAdapter
from pipeline.config_loader import SparkConfig
from pipeline.schemas import BRONZE_ACCOUNTS_SCHEMA
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA
from pipeline.schemas import BRONZE_TRANSACTIONS_SCHEMA
from pipeline.spark_utils import build_spark_session


class PySparkBronzeAdapter(BronzeIngestAdapter):
    engine_name = "pyspark_delta"

    def __init__(self, spark_config: SparkConfig) -> None:
        self._spark = build_spark_session(spark_config)

    def ingest_customers(
        self,
        *,
        input_path: str,
        output_path: str,
        run_timestamp: datetime,
    ) -> None:
        from pyspark.sql import functions as F

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        customers_df = (
            self._spark.read.option("header", True)
            .schema(BRONZE_CUSTOMERS_SCHEMA)
            .csv(input_path)
        )
        (
            customers_df.withColumn(
                "ingestion_timestamp",
                F.lit(run_timestamp).cast("timestamp"),
            )
            .write.format("delta")
            .mode("overwrite")
            .save(output_path)
        )

    def ingest_accounts(
        self,
        *,
        input_path: str,
        output_path: str,
        run_timestamp: datetime,
    ) -> None:
        from pyspark.sql import functions as F

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        accounts_df = (
            self._spark.read.option("header", True)
            .schema(BRONZE_ACCOUNTS_SCHEMA)
            .csv(input_path)
        )
        (
            accounts_df.withColumn(
                "ingestion_timestamp",
                F.lit(run_timestamp).cast("timestamp"),
            )
            .write.format("delta")
            .mode("overwrite")
            .save(output_path)
        )

    def ingest_transactions(
        self,
        *,
        input_path: str,
        output_path: str,
        run_timestamp: datetime,
    ) -> None:
        from pyspark.sql import functions as F

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        transactions_df = self._spark.read.schema(BRONZE_TRANSACTIONS_SCHEMA).json(
            input_path
        )
        (
            transactions_df.withColumn(
                "ingestion_timestamp",
                F.lit(run_timestamp).cast("timestamp"),
            )
            .write.format("delta")
            .mode("overwrite")
            .save(output_path)
        )

    def close(self) -> None:
        self._spark.stop()
