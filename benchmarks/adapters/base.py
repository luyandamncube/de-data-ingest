"""Base adapter contract for benchmark engines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from benchmarks.implementation_resolver import ResolvedImplementation
from pipeline.registry import TrackingUnit


@dataclass(slots=True)
class BenchmarkExecution:
    workload_id: str
    engine: str
    elapsed_seconds: float
    peak_memory_mb: float | None
    validation_seconds: float | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    cpu_user_seconds: float | None = None
    cpu_pct: float | None = None
    delta_write_seconds: float | None = None
    delta_read_seconds: float | None = None
    tmp_peak_mb: float | None = None
    output_file_count: int | None = None
    delta_roundtrip_ok: bool = False
    notes: str = ""
    artifacts: dict[str, Any] = field(default_factory=dict)


class EngineAdapter(ABC):
    """Minimal contract every benchmark engine adapter must satisfy."""

    name: str

    @abstractmethod
    def is_available(self) -> bool:
        """Return whether the adapter can run in the current environment."""

    @abstractmethod
    def supported_families(self) -> tuple[str, ...]:
        """Return the workload families this adapter knows how to execute."""

    @abstractmethod
    def run(
        self,
        workload: TrackingUnit,
        dataset_profile: str,
        implementation: ResolvedImplementation,
        *,
        attempt: int,
    ) -> BenchmarkExecution:
        """Run the supplied workload against the named dataset profile."""
