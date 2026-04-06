"""Typed pipeline config loading helpers."""

from __future__ import annotations

from dataclasses import dataclass
import os
import posixpath

import yaml


DEFAULT_CONFIG_PATH = "/data/config/pipeline_config.yaml"


@dataclass(frozen=True, slots=True)
class InputConfig:
    accounts_path: str
    transactions_path: str
    customers_path: str


@dataclass(frozen=True, slots=True)
class OutputConfig:
    bronze_path: str
    silver_path: str
    gold_path: str
    dq_report_path: str | None = None


@dataclass(frozen=True, slots=True)
class SparkConfig:
    master: str
    app_name: str
    local_dir: str | None = None


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    input: InputConfig
    output: OutputConfig
    spark: SparkConfig

    def bronze_table_path(self, table_name: str) -> str:
        return posixpath.join(self.output.bronze_path, table_name)

    def silver_table_path(self, table_name: str) -> str:
        return posixpath.join(self.output.silver_path, table_name)

    def gold_table_path(self, table_name: str) -> str:
        return posixpath.join(self.output.gold_path, table_name)


def load_config(config_path: str | None = None) -> PipelineConfig:
    resolved_path = config_path or os.environ.get("PIPELINE_CONFIG", DEFAULT_CONFIG_PATH)
    with open(resolved_path, encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    input_section = _require_mapping(raw, "input")
    output_section = _require_mapping(raw, "output")
    spark_section = _require_mapping(raw, "spark")

    return PipelineConfig(
        input=InputConfig(
            accounts_path=_require_string(input_section, "accounts_path"),
            transactions_path=_require_string(input_section, "transactions_path"),
            customers_path=_require_string(input_section, "customers_path"),
        ),
        output=OutputConfig(
            bronze_path=_require_string(output_section, "bronze_path"),
            silver_path=_require_string(output_section, "silver_path"),
            gold_path=_require_string(output_section, "gold_path"),
            dq_report_path=_optional_string(output_section, "dq_report_path"),
        ),
        spark=SparkConfig(
            master=_require_string(spark_section, "master"),
            app_name=_require_string(spark_section, "app_name"),
            local_dir=_optional_string(spark_section, "local_dir"),
        ),
    )


def _require_mapping(data: dict[str, object], key: str) -> dict[str, object]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"config section '{key}' is missing or not a mapping")
    return value


def _require_string(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"config value '{key}' is missing or not a non-empty string")
    return value


def _optional_string(data: dict[str, object], key: str) -> str | None:
    value = data.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"config value '{key}' is not a non-empty string")
    return value
