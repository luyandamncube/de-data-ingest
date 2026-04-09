"""Hard validation gates for Stage 1 Bronze outputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from deltalake import DeltaTable
import polars as pl
from pyspark.sql import types as T

from pipeline.config_loader import PipelineConfig
from pipeline.schemas import BRONZE_ACCOUNTS_SCHEMA
from pipeline.schemas import BRONZE_CUSTOMERS_SCHEMA
from pipeline.schemas import BRONZE_TRANSACTIONS_SCHEMA


class BronzeValidationError(RuntimeError):
    """Raised when a Bronze validation gate fails."""


@dataclass(frozen=True, slots=True)
class BronzeTableSpec:
    name: str
    input_path: str
    output_path: str
    expected_schema: T.StructType
    row_counter: Callable[[Path], int]


def validate_bronze_outputs(
    *,
    config: PipelineConfig,
    run_timestamp: datetime,
) -> None:
    """Fail hard if Bronze outputs do not satisfy the Stage 1 contract."""

    specs = (
        BronzeTableSpec(
            name="customers",
            input_path=config.input.customers_path,
            output_path=config.bronze_table_path("customers"),
            expected_schema=BRONZE_CUSTOMERS_SCHEMA,
            row_counter=_count_csv_rows,
        ),
        BronzeTableSpec(
            name="accounts",
            input_path=config.input.accounts_path,
            output_path=config.bronze_table_path("accounts"),
            expected_schema=BRONZE_ACCOUNTS_SCHEMA,
            row_counter=_count_csv_rows,
        ),
        BronzeTableSpec(
            name="transactions",
            input_path=config.input.transactions_path,
            output_path=config.bronze_table_path("transactions"),
            expected_schema=BRONZE_TRANSACTIONS_SCHEMA,
            row_counter=_count_jsonl_rows,
        ),
    )

    table_timestamps: dict[str, datetime] = {}
    for spec in specs:
        table_timestamps[spec.name] = _validate_bronze_table(spec)

    for table_name, observed_timestamp in table_timestamps.items():
        if observed_timestamp != run_timestamp:
            raise BronzeValidationError(
                f"{table_name} ingestion_timestamp {observed_timestamp!s} "
                f"does not match run timestamp {run_timestamp!s}"
            )

    if len(set(table_timestamps.values())) != 1:
        raise BronzeValidationError(
            "Bronze tables do not share one consistent ingestion_timestamp"
        )


def _validate_bronze_table(spec: BronzeTableSpec) -> datetime:
    output_path = Path(spec.output_path)
    if not output_path.exists():
        raise BronzeValidationError(
            f"Bronze output path does not exist for {spec.name}: {output_path}"
        )

    delta_log_path = output_path / "_delta_log"
    if not delta_log_path.exists():
        raise BronzeValidationError(
            f"Bronze Delta log is missing for {spec.name}: {delta_log_path}"
        )

    input_path = Path(spec.input_path)
    if not input_path.exists():
        raise BronzeValidationError(
            f"Bronze source input path does not exist for {spec.name}: {input_path}"
        )

    expected_rows = spec.row_counter(input_path)
    delta_table = DeltaTable(str(output_path))
    active_files = _active_file_uris(delta_table, output_path)
    if not active_files:
        raise BronzeValidationError(
            f"Bronze Delta table for {spec.name} contains no active parquet files"
        )

    _validate_delta_schema(
        table_name=spec.name,
        expected_schema=spec.expected_schema,
        actual_delta_schema=delta_table.schema().json(),
    )

    parquet_scan = pl.scan_parquet(active_files)
    actual_rows = parquet_scan.select(pl.len().alias("row_count")).collect().item()
    if actual_rows != expected_rows:
        raise BronzeValidationError(
            f"{spec.name} row count mismatch: expected {expected_rows}, got {actual_rows}"
        )

    timestamp_row = (
        parquet_scan.select(
            pl.col("ingestion_timestamp").min().alias("min_ingestion_timestamp"),
            pl.col("ingestion_timestamp").max().alias("max_ingestion_timestamp"),
        )
        .collect()
        .row(0, named=True)
    )
    min_ingestion_timestamp = timestamp_row["min_ingestion_timestamp"]
    max_ingestion_timestamp = timestamp_row["max_ingestion_timestamp"]
    if min_ingestion_timestamp is None or max_ingestion_timestamp is None:
        raise BronzeValidationError(
            f"{spec.name} ingestion_timestamp column is empty or null-only"
        )
    if min_ingestion_timestamp != max_ingestion_timestamp:
        raise BronzeValidationError(
            f"{spec.name} has multiple ingestion_timestamp values: "
            f"{min_ingestion_timestamp!s} != {max_ingestion_timestamp!s}"
        )

    return min_ingestion_timestamp


def _active_file_uris(delta_table: DeltaTable, output_path: Path) -> list[str]:
    if hasattr(delta_table, "file_uris"):
        return list(delta_table.file_uris())
    return [str(output_path / relative_path) for relative_path in delta_table.files()]


def _validate_delta_schema(
    *,
    table_name: str,
    expected_schema: T.StructType,
    actual_delta_schema: dict[str, object],
) -> None:
    expected_names = [field.name for field in expected_schema.fields] + [
        "ingestion_timestamp"
    ]
    actual_fields = actual_delta_schema.get("fields")
    if not isinstance(actual_fields, list):
        raise BronzeValidationError(
            f"{table_name} Delta schema is malformed: {actual_delta_schema}"
        )

    actual_names = [str(field.get("name")) for field in actual_fields]
    if actual_names != expected_names:
        raise BronzeValidationError(
            f"{table_name} columns do not match expected Bronze contract: "
            f"expected {expected_names}, got {actual_names}"
        )

    for expected_field, actual_field in zip(expected_schema.fields, actual_fields):
        if not isinstance(actual_field, dict):
            raise BronzeValidationError(
                f"{table_name} Delta field entry is malformed: {actual_field}"
            )
        _validate_field_type(
            table_name=table_name,
            field_path=expected_field.name,
            expected_type=expected_field.dataType,
            actual_type=actual_field.get("type"),
        )

    timestamp_type = actual_fields[-1].get("type")
    if not isinstance(timestamp_type, str) or not timestamp_type.startswith("timestamp"):
        raise BronzeValidationError(
            f"{table_name}.ingestion_timestamp is not a timestamp type: {timestamp_type}"
        )


def _validate_field_type(
    *,
    table_name: str,
    field_path: str,
    expected_type: T.DataType,
    actual_type: object,
) -> None:
    if isinstance(expected_type, T.StringType):
        if actual_type == "string":
            return
    elif isinstance(expected_type, T.IntegerType):
        if actual_type in {"integer", "long"}:
            return
    elif isinstance(expected_type, T.DecimalType):
        expected_decimal = f"decimal({expected_type.precision},{expected_type.scale})"
        if actual_type == expected_decimal:
            return
    elif isinstance(expected_type, T.BooleanType):
        if actual_type == "boolean":
            return
    elif isinstance(expected_type, T.StructType):
        if not isinstance(actual_type, dict) or actual_type.get("type") != "struct":
            raise BronzeValidationError(
                f"{table_name}.{field_path} is not a struct type: {actual_type}"
            )

        actual_fields = actual_type.get("fields")
        if not isinstance(actual_fields, list):
            raise BronzeValidationError(
                f"{table_name}.{field_path} struct field metadata is malformed: "
                f"{actual_type}"
            )
        expected_child_names = [field.name for field in expected_type.fields]
        actual_child_names = [str(field.get("name")) for field in actual_fields]
        if actual_child_names != expected_child_names:
            raise BronzeValidationError(
                f"{table_name}.{field_path} struct fields do not match: "
                f"expected {expected_child_names}, got {actual_child_names}"
            )

        for expected_child, actual_child in zip(expected_type.fields, actual_fields):
            if not isinstance(actual_child, dict):
                raise BronzeValidationError(
                    f"{table_name}.{field_path} struct field is malformed: {actual_child}"
                )
            _validate_field_type(
                table_name=table_name,
                field_path=f"{field_path}.{expected_child.name}",
                expected_type=expected_child.dataType,
                actual_type=actual_child.get("type"),
            )
        return

    raise BronzeValidationError(
        f"{table_name}.{field_path} type mismatch: expected {expected_type}, got {actual_type}"
    )


def _count_csv_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def _count_jsonl_rows(path: Path) -> int:
    with path.open(encoding="utf-8") as handle:
        return sum(1 for _ in handle)
