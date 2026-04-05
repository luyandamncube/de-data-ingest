"""Benchmark result and matrix serialization helpers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
import json


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    workload_id: str
    engine: str
    dataset_profile: str
    correct: bool
    elapsed_seconds: float
    peak_memory_mb: float | None
    delta_roundtrip_ok: bool
    notes: str = ""
    selected: bool = False

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(slots=True)
class BenchmarkMatrix:
    generated_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat(timespec="seconds")
    )
    results: list[BenchmarkResult] = field(default_factory=list)

    def add_result(self, result: BenchmarkResult) -> None:
        self.results.append(result)

    def as_dict(self) -> dict[str, object]:
        return {
            "generated_at": self.generated_at,
            "results": [result.as_dict() for result in self.results],
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)


def build_sample_result(
    workload_id: str,
    engine: str,
    *,
    dataset_profile: str = "smoke",
    notes: str = "benchmark skeleton sample result",
) -> BenchmarkResult:
    """Build a deterministic sample result for smoke validation."""

    return BenchmarkResult(
        workload_id=workload_id,
        engine=engine,
        dataset_profile=dataset_profile,
        correct=True,
        elapsed_seconds=0.0,
        peak_memory_mb=None,
        delta_roundtrip_ok=False,
        notes=notes,
        selected=False,
    )
