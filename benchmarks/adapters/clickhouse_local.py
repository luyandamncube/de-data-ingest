"""clickhouse-local adapter for benchmark workloads."""

from __future__ import annotations

import json
from pathlib import Path
import resource
import shutil
import subprocess
import time

from benchmarks.adapters.base import BenchmarkExecution, EngineAdapter
from benchmarks.implementation_resolver import ResolvedImplementation
from pipeline.registry import TrackingUnit


class ClickHouseLocalAdapter(EngineAdapter):
    """Adapter that can execute workloads with clickhouse-local."""

    CLICKHOUSE_SOURCE = Path("/usr/local/bin/clickhouse")
    CLICKHOUSE_TMP = Path("/benchmarks/results/clickhouse")

    def __init__(self) -> None:
        self.name = "clickhouse_local"

    def is_available(self) -> bool:
        binary = self._prepare_clickhouse_binary()
        completed = subprocess.run(
            [str(binary), "local", "--version"],
            check=False,
            capture_output=True,
            text=True,
            cwd="/tmp",
        )
        return completed.returncode == 0

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
            execution = implementation.callable_obj(
                engine_name=self.name,
                workload=workload,
                dataset_profile=dataset_profile,
                attempt=attempt,
            )
            execution.notes = (
                f"{execution.notes}; adapter=clickhouse_local; impl={implementation.ref}"
            )
            execution.artifacts.setdefault("clickhouse_local_enabled", False)
            return execution
        if implementation.kind == "sql" and implementation.sql_path is not None:
            return self._run_sql_workload(
                workload,
                dataset_profile,
                implementation,
                attempt=attempt,
            )
        raise NotImplementedError(
            f"unsupported implementation kind for clickhouse_local: {implementation.kind}"
        )

    def _run_sql_workload(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
    ) -> BenchmarkExecution:
        if workload.id != "BM_07":
            raise NotImplementedError(
                f"sql input registration is not configured for workload {workload.id}"
            )

        binary = self._prepare_clickhouse_binary()
        sql_text = Path(implementation.sql_path).read_text(encoding="utf-8").strip()
        rows_in = self._count_rows(workload)

        cpu_start = self._cpu_user_seconds()
        start = time.perf_counter()
        completed = subprocess.run(
            [str(binary), "local", "--query", sql_text],
            check=False,
            capture_output=True,
            text=True,
            cwd="/tmp",
        )
        elapsed_seconds = time.perf_counter() - start
        cpu_user_seconds = self._cpu_user_seconds() - cpu_start

        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()
            raise RuntimeError(stderr or "clickhouse-local query failed")

        rows = []
        for line in completed.stdout.splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))

        cpu_pct = (
            (cpu_user_seconds / elapsed_seconds) * 100.0
            if elapsed_seconds > 0
            else None
        )

        return BenchmarkExecution(
            workload_id=workload.id,
            engine=self.name,
            elapsed_seconds=elapsed_seconds,
            peak_memory_mb=self._peak_memory_mb(),
            rows_in=rows_in,
            rows_out=len(rows),
            cpu_user_seconds=cpu_user_seconds,
            cpu_pct=cpu_pct,
            delta_write_seconds=None,
            delta_read_seconds=None,
            tmp_peak_mb=None,
            output_file_count=0,
            delta_roundtrip_ok=False,
            notes=(
                "sql-backed clickhouse-local workload"
                f"; dataset_profile={dataset_profile}"
                f"; attempt={attempt}"
                f"; impl={implementation.ref}"
            ),
            artifacts={
                "sql_path": implementation.sql_path,
                "preview": rows[:3],
                "registered_tables": ["customers"],
                "clickhouse_local_enabled": True,
            },
        )

    def _count_rows(self, workload: TrackingUnit) -> int:
        if workload.id != "BM_07":
            raise NotImplementedError(
                f"row count registration is not configured for workload {workload.id}"
            )

        binary = self._prepare_clickhouse_binary()
        customers_path = "/benchmarks/data/customers.csv"
        count_query = (
            "SELECT count() AS row_count "
            f"FROM file('{customers_path}', CSVWithNames)"
            " FORMAT JSONEachRow"
        )
        completed = subprocess.run(
            [str(binary), "local", "--query", count_query],
            check=False,
            capture_output=True,
            text=True,
            cwd="/tmp",
        )
        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()
            raise RuntimeError(stderr or "clickhouse-local count query failed")
        payload = json.loads(completed.stdout.strip())
        return int(payload["row_count"])

    def _prepare_clickhouse_binary(self) -> Path:
        if not self.CLICKHOUSE_SOURCE.exists():
            raise FileNotFoundError(
                f"clickhouse binary not found: {self.CLICKHOUSE_SOURCE}"
            )

        self.CLICKHOUSE_TMP.parent.mkdir(parents=True, exist_ok=True)
        if not self.CLICKHOUSE_TMP.exists():
            shutil.copy2(self.CLICKHOUSE_SOURCE, self.CLICKHOUSE_TMP)
            self.CLICKHOUSE_TMP.chmod(0o755)

        return self.CLICKHOUSE_TMP

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
