"""Single-run benchmark entrypoint executed inside the benchmark container."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from benchmarks.adapters.factory import get_adapter
from benchmarks.implementation_resolver import resolve_implementation
from benchmarks.results import BenchmarkResult
from pipeline.registry import filter_manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Execute one benchmark run inside the benchmark container."
    )
    parser.add_argument("--engine", required=True)
    parser.add_argument("--workload-id", required=True)
    parser.add_argument("--dataset-profile", default="smoke")
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--result-path", required=True)
    return parser


def load_workload(workload_id: str):
    manifest = {item.id: item for item in filter_manifest()}
    if workload_id not in manifest:
        raise SystemExit(f"unknown workload id: {workload_id}")
    return manifest[workload_id]


def main() -> int:
    args = build_parser().parse_args()
    workload = load_workload(args.workload_id)
    if workload.candidate_engines and args.engine not in workload.candidate_engines:
        raise SystemExit(
            f"engine {args.engine} is not registered for workload {workload.id}"
        )
    implementation = resolve_implementation(workload, engine=args.engine)

    adapter = get_adapter(args.engine)
    execution = adapter.run(
        workload,
        args.dataset_profile,
        implementation,
        attempt=args.attempt,
    )

    result = BenchmarkResult(
        workload_id=execution.workload_id,
        engine=execution.engine,
        dataset_profile=args.dataset_profile,
        correct=True,
        exit_code=0,
        failure_type=None,
        elapsed_seconds=execution.elapsed_seconds,
        workload_exec_seconds=execution.elapsed_seconds,
        validation_seconds=execution.validation_seconds,
        cold_start_seconds=0.0,
        peak_memory_mb=execution.peak_memory_mb,
        tmp_peak_mb=execution.tmp_peak_mb,
        rows_in=execution.rows_in,
        rows_out=execution.rows_out,
        cpu_user_seconds=execution.cpu_user_seconds,
        cpu_pct=execution.cpu_pct,
        image_size_mb=None,
        delta_write_seconds=execution.delta_write_seconds,
        delta_read_seconds=execution.delta_read_seconds,
        delta_roundtrip_ok=execution.delta_roundtrip_ok,
        output_file_count=execution.output_file_count,
        notes=f"{execution.notes}; attempt={args.attempt}",
        selected=False,
    )

    result_path = Path(args.result_path)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        json.dumps(result.as_dict(), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    result_path.chmod(0o666)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
