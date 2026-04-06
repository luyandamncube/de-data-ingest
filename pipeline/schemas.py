"""Explicit source schemas used by the batch pipeline."""

from __future__ import annotations

from pyspark.sql import types as T


# Bronze stays as close to the landed source as possible. CSV fields remain
# strings, and JSON leaves are modeled as strings so Stage 2 type variants can
# still be ingested without silently nulling values on read.
BRONZE_CUSTOMERS_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("id_number", T.StringType(), True),
        T.StructField("first_name", T.StringType(), True),
        T.StructField("last_name", T.StringType(), True),
        T.StructField("dob", T.StringType(), True),
        T.StructField("gender", T.StringType(), True),
        T.StructField("province", T.StringType(), True),
        T.StructField("income_band", T.StringType(), True),
        T.StructField("segment", T.StringType(), True),
        T.StructField("risk_score", T.IntegerType(), True),
        T.StructField("kyc_status", T.StringType(), True),
        T.StructField("product_flags", T.StringType(), True),
    ]
)


BRONZE_ACCOUNTS_SCHEMA = T.StructType(
    [
        T.StructField("account_id", T.StringType(), True),
        T.StructField("customer_ref", T.StringType(), True),
        T.StructField("account_type", T.StringType(), True),
        T.StructField("account_status", T.StringType(), True),
        T.StructField("open_date", T.StringType(), True),
        T.StructField("product_tier", T.StringType(), True),
        T.StructField("mobile_number", T.StringType(), True),
        T.StructField("digital_channel", T.StringType(), True),
        T.StructField("credit_limit", T.DecimalType(18, 2), True),
        T.StructField("current_balance", T.DecimalType(18, 2), True),
        T.StructField("last_activity_date", T.StringType(), True),
    ]
)


BRONZE_TRANSACTIONS_SCHEMA = T.StructType(
    [
        T.StructField("transaction_id", T.StringType(), True),
        T.StructField("account_id", T.StringType(), True),
        T.StructField("transaction_date", T.StringType(), True),
        T.StructField("transaction_time", T.StringType(), True),
        T.StructField("transaction_type", T.StringType(), True),
        T.StructField("merchant_category", T.StringType(), True),
        T.StructField("merchant_subcategory", T.StringType(), True),
        T.StructField("amount", T.StringType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("channel", T.StringType(), True),
        T.StructField(
            "location",
            T.StructType(
                [
                    T.StructField("province", T.StringType(), True),
                    T.StructField("city", T.StringType(), True),
                    T.StructField("coordinates", T.StringType(), True),
                ]
            ),
            True,
        ),
        T.StructField(
            "metadata",
            T.StructType(
                [
                    T.StructField("device_id", T.StringType(), True),
                    T.StructField("session_id", T.StringType(), True),
                    T.StructField("retry_flag", T.StringType(), True),
                ]
            ),
            True,
        ),
    ]
)
