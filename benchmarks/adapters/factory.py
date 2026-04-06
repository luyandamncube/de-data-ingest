"""Factory helpers for benchmark engine adapters."""

from __future__ import annotations

from benchmarks.adapters.base import EngineAdapter
from benchmarks.adapters.clickhouse_local import ClickHouseLocalAdapter
from benchmarks.adapters.datafusion import DataFusionAdapter
from benchmarks.adapters.duckdb import DuckDBAdapter
from benchmarks.adapters.polars import PolarsAdapter
from benchmarks.adapters.pyspark_delta import PySparkDeltaAdapter
from benchmarks.adapters.simulated import SimulatedAdapter
from pipeline.registry import SHORTLIST_ENGINES


def get_adapter(engine: str) -> EngineAdapter:
    """Return an adapter for the requested engine name."""

    if engine not in SHORTLIST_ENGINES:
        raise ValueError(f"unsupported engine: {engine}")
    if engine == "pyspark_delta":
        return PySparkDeltaAdapter()
    if engine == "polars":
        return PolarsAdapter()
    if engine == "datafusion":
        return DataFusionAdapter()
    if engine == "duckdb":
        return DuckDBAdapter()
    if engine == "clickhouse_local":
        return ClickHouseLocalAdapter()
    return SimulatedAdapter(engine)
