"""Polars-backed adapter for running benchmark workloads."""

from __future__ import annotations

from pathlib import Path
import resource
import time

from benchmarks.adapters.base import BenchmarkExecution, EngineAdapter
from benchmarks.implementation_resolver import ResolvedImplementation
from pipeline.registry import TrackingUnit


class PolarsAdapter(EngineAdapter):
    """Adapter that can execute workloads with Polars."""

    def __init__(self) -> None:
        self.name = "polars"

    def is_available(self) -> bool:
        try:
            import polars  # noqa: F401
        except ImportError:
            return False
        return True

    def supported_families(self) -> tuple[str, ...]:
        return (
            "docker_smoke",
            "bronze_ingest",
            "silver_standardisation",
            "silver_dedup",
            "silver_linkage",
        )

    def run(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
    ) -> BenchmarkExecution:
        if implementation.kind == "python" and implementation.callable_obj is not None:
            return self._run_python_workload(
                workload,
                dataset_profile,
                implementation,
                attempt=attempt,
            )
        if implementation.kind == "sql" and implementation.sql_path is not None:
            return self._run_sql_workload(
                workload,
                dataset_profile,
                implementation,
                attempt=attempt,
            )
        raise NotImplementedError(
            f"unsupported implementation kind for polars: {implementation.kind}"
        )

    def _run_python_workload(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
    ) -> BenchmarkExecution:
        execution = implementation.callable_obj(
            engine_name=self.name,
            workload=workload,
            dataset_profile=dataset_profile,
            attempt=attempt,
            data_root=Path("/benchmarks/data"),
        )
        execution.notes = (
            f"{execution.notes}; adapter=polars; impl={implementation.ref}"
        )
        execution.artifacts.setdefault("polars_enabled", False)
        return execution

    def _run_sql_workload(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
    ) -> BenchmarkExecution:
        import polars as pl

        sql_text = Path(implementation.sql_path).read_text(encoding="utf-8")
        data_root = Path("/benchmarks/data")
        rows_in, sql_context = self._register_sql_inputs(workload, data_root, pl)

        cpu_start = self._cpu_user_seconds()
        start = time.perf_counter()
        result = sql_context.execute(sql_text)
        result_df = result.collect() if hasattr(result, "collect") else result
        elapsed_seconds = time.perf_counter() - start
        cpu_user_seconds = self._cpu_user_seconds() - cpu_start
        cpu_pct = (
            (cpu_user_seconds / elapsed_seconds) * 100.0
            if elapsed_seconds > 0
            else None
        )
        rows_out = result_df.height

        return BenchmarkExecution(
            workload_id=workload.id,
            engine=self.name,
            elapsed_seconds=elapsed_seconds,
            peak_memory_mb=self._peak_memory_mb(),
            rows_in=rows_in,
            rows_out=rows_out,
            cpu_user_seconds=cpu_user_seconds,
            cpu_pct=cpu_pct,
            delta_write_seconds=None,
            delta_read_seconds=None,
            tmp_peak_mb=None,
            output_file_count=0,
            delta_roundtrip_ok=False,
            notes=(
                "sql-backed Polars workload"
                f"; dataset_profile={dataset_profile}"
                f"; attempt={attempt}"
                f"; impl={implementation.ref}"
            ),
            artifacts={
                "sql_path": implementation.sql_path,
                "preview": result_df.head(3).to_dicts(),
                "registered_tables": ["customers"],
                "polars_enabled": True,
            },
        )

    def _register_sql_inputs(self, workload: TrackingUnit, data_root: Path, pl):
        if workload.id != "BM_07":
            raise NotImplementedError(
                f"sql input registration is not configured for workload {workload.id}"
            )

        customers_path = data_root / "customers.csv"
        if not customers_path.exists():
            raise FileNotFoundError(f"customers dataset not found: {customers_path}")

        customers = pl.scan_csv(str(customers_path)).select("province")
        rows_in = customers.select(pl.len().alias("row_count")).collect().item()
        sql_context = pl.SQLContext(customers=customers)
        return rows_in, sql_context

    def _peak_memory_mb(self) -> float:
        self_usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        children_usage = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        return max(self_usage, children_usage) / 1024.0

    def _cpu_user_seconds(self) -> float:
        self_usage = resource.getrusage(resource.RUSAGE_SELF)
        children_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        return (
            self_usage.ru_utime
            + self_usage.ru_stime
            + children_usage.ru_utime
            + children_usage.ru_stime
        )
