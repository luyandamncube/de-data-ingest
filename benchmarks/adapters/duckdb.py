"""DuckDB-backed adapter for running benchmark workloads."""

from __future__ import annotations

from pathlib import Path
import resource
import time

from benchmarks.adapters.base import BenchmarkExecution, EngineAdapter
from benchmarks.implementation_resolver import ResolvedImplementation
from pipeline.registry import TrackingUnit


class DuckDBAdapter(EngineAdapter):
    """Adapter that can execute workloads with DuckDB."""

    def __init__(self) -> None:
        self.name = "duckdb"

    def is_available(self) -> bool:
        try:
            import duckdb  # noqa: F401
        except ImportError:
            return False
        return True

    def supported_families(self) -> tuple[str, ...]:
        return ("docker_smoke",)

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
            f"unsupported implementation kind for duckdb: {implementation.kind}"
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
        )
        execution.notes = f"{execution.notes}; adapter=duckdb; impl={implementation.ref}"
        execution.artifacts.setdefault("duckdb_enabled", False)
        return execution

    def _run_sql_workload(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
    ) -> BenchmarkExecution:
        import duckdb
        import pyarrow as pa

        sql_text = Path(implementation.sql_path).read_text(encoding="utf-8")
        data_root = Path("/benchmarks/data")
        connection = duckdb.connect(database=":memory:")

        try:
            rows_in = self._register_sql_inputs(workload, data_root, connection)

            cpu_start = self._cpu_user_seconds()
            start = time.perf_counter()
            result_table = connection.execute(sql_text).fetch_arrow_table()
            elapsed_seconds = time.perf_counter() - start
            cpu_user_seconds = self._cpu_user_seconds() - cpu_start
            cpu_pct = (
                (cpu_user_seconds / elapsed_seconds) * 100.0
                if elapsed_seconds > 0
                else None
            )

            if not isinstance(result_table, pa.Table):
                result_table = pa.table({})

            return BenchmarkExecution(
                workload_id=workload.id,
                engine=self.name,
                elapsed_seconds=elapsed_seconds,
                peak_memory_mb=self._peak_memory_mb(),
                rows_in=rows_in,
                rows_out=result_table.num_rows,
                cpu_user_seconds=cpu_user_seconds,
                cpu_pct=cpu_pct,
                delta_write_seconds=None,
                delta_read_seconds=None,
                tmp_peak_mb=None,
                output_file_count=0,
                delta_roundtrip_ok=False,
                notes=(
                    "sql-backed DuckDB workload"
                    f"; dataset_profile={dataset_profile}"
                    f"; attempt={attempt}"
                    f"; impl={implementation.ref}"
                ),
                artifacts={
                    "sql_path": implementation.sql_path,
                    "preview": result_table.slice(0, 3).to_pylist(),
                    "registered_tables": ["customers"],
                    "duckdb_enabled": True,
                },
            )
        finally:
            connection.close()

    def _register_sql_inputs(self, workload: TrackingUnit, data_root: Path, connection) -> int:
        if workload.id != "BM_07":
            raise NotImplementedError(
                f"sql input registration is not configured for workload {workload.id}"
            )

        customers_path = data_root / "customers.csv"
        if not customers_path.exists():
            raise FileNotFoundError(f"customers dataset not found: {customers_path}")

        escaped_path = str(customers_path).replace("'", "''")
        connection.execute(
            f"""
            CREATE VIEW customers AS
            SELECT *
            FROM read_csv_auto('{escaped_path}', header=true)
            """
        )
        row_count = connection.execute(
            "SELECT COUNT(*) AS row_count FROM customers"
        ).fetchone()
        return int(row_count[0]) if row_count else 0

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
