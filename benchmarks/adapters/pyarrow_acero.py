"""PyArrow/Acero-backed adapter for benchmark workloads."""

from __future__ import annotations

from pathlib import Path

from benchmarks.adapters.base import BenchmarkExecution, EngineAdapter
from benchmarks.implementation_resolver import ResolvedImplementation
from pipeline.registry import TrackingUnit


class PyArrowAceroAdapter(EngineAdapter):
    """Adapter that executes engine-specific Python workloads with PyArrow."""

    def __init__(self) -> None:
        self.name = "pyarrow_acero"

    def is_available(self) -> bool:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            return False
        return True

    def supported_families(self) -> tuple[str, ...]:
        return ("docker_smoke", "bronze_ingest")

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
                "pyarrow_acero currently supports python-backed benchmark workloads"
            )

        execution = implementation.callable_obj(
            engine_name=self.name,
            workload=workload,
            dataset_profile=dataset_profile,
            attempt=attempt,
            data_root=Path("/benchmarks/data"),
        )
        execution.notes = (
            f"{execution.notes}; adapter=pyarrow_acero; impl={implementation.ref}"
        )
        execution.artifacts.setdefault("pyarrow_acero_enabled", True)
        return execution
