"""Dummy Docker smoke workload used to validate the benchmark loop."""

from __future__ import annotations

import hashlib
import time

from benchmarks.adapters.base import BenchmarkExecution
from pipeline.registry import TrackingUnit


def run(
    *,
    engine_name: str,
    workload: TrackingUnit,
    dataset_profile: str,
    attempt: int,
    **_: object,
) -> BenchmarkExecution:
    """Execute the BM_06 dummy smoke workload.
    """

    token = f"{engine_name}:{workload.id}:{dataset_profile}:{attempt}"
    digest = hashlib.sha256(token.encode("utf-8")).digest()
    sleep_ms = 15 + (digest[0] % 35)
    peak_memory_mb = 32.0 + float(digest[1] % 24)

    cpu_start = time.process_time()
    start = time.perf_counter()
    time.sleep(sleep_ms / 1000.0)
    elapsed_seconds = time.perf_counter() - start
    cpu_user_seconds = time.process_time() - cpu_start
    cpu_pct = (
        (cpu_user_seconds / elapsed_seconds) * 100.0 if elapsed_seconds > 0 else None
    )

    return BenchmarkExecution(
        workload_id=workload.id,
        engine=engine_name,
        elapsed_seconds=elapsed_seconds,
        peak_memory_mb=peak_memory_mb,
        rows_in=0,
        rows_out=0,
        cpu_user_seconds=cpu_user_seconds,
        cpu_pct=cpu_pct,
        delta_write_seconds=None,
        delta_read_seconds=None,
        tmp_peak_mb=None,
        output_file_count=0,
        delta_roundtrip_ok=False,
        notes="file-backed BM_06 docker smoke workload",
        artifacts={
            "dataset_profile": dataset_profile,
            "implementation_ref": (
                "benchmarks.workloads.smoke.bm_06_docker_smoke:run"
            ),
        },
    )
