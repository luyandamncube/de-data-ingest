"""Deterministic smoke adapter used before real engine adapters exist."""

from __future__ import annotations

import time

from benchmarks.adapters.base import BenchmarkExecution, EngineAdapter
from benchmarks.implementation_resolver import ResolvedImplementation
from pipeline.registry import TrackingUnit


class SimulatedAdapter(EngineAdapter):
    """Placeholder adapter that simulates a benchmark run deterministically."""

    def __init__(self, engine_name: str) -> None:
        self.name = engine_name

    def is_available(self) -> bool:
        return True

    def supported_families(self) -> tuple[str, ...]:
        return ("benchmark_platform", "docker_smoke")

    def run(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
    ) -> BenchmarkExecution:
        if implementation.kind != "python" or implementation.callable_obj is None:
            raise NotImplementedError(
                f"{self.name} adapter only supports python-backed workloads today"
            )
        start = time.perf_counter()
        execution = implementation.callable_obj(
            engine_name=self.name,
            workload=workload,
            dataset_profile=dataset_profile,
            attempt=attempt,
        )
        elapsed_seconds = time.perf_counter() - start
        if execution.elapsed_seconds <= 0:
            execution.elapsed_seconds = elapsed_seconds
        execution.notes = (
            f"{execution.notes}; adapter=simulated; impl={implementation.ref}"
        )
        execution.artifacts.setdefault("simulated", True)
        return execution
