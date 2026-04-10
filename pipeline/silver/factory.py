"""Factory helpers for Silver transform adapters."""

from __future__ import annotations

from pipeline.silver.base import SilverTransformAdapter
from pipeline.silver.polars_adapter import PolarsSilverAdapter


def build_silver_adapter(engine_name: str) -> SilverTransformAdapter:
    if engine_name == "polars":
        return PolarsSilverAdapter()
    raise ValueError(f"unsupported Silver transform engine: {engine_name}")
