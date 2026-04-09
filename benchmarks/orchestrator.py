"""Host-side Docker orchestrator for benchmark runs."""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import re
import statistics
import subprocess
import sys
import tempfile
import time

from benchmarks.results import BenchmarkResult
from pipeline.registry import SHORTLIST_ENGINES, filter_manifest


DEFAULT_BENCHMARK_IMAGE = "de-data-ingest-bench:test"
DEFAULT_DOCKERFILE = "benchmarks/Dockerfile"
DEFAULT_SMOKE_WORKLOAD = "BM_06"
STATS_POLL_SECONDS = 1.0


def normalise_csv_args(values: list[str] | None) -> list[str]:
    if not values:
        return []

    normalised: list[str] = []
    for value in values:
        for item in value.split(","):
            candidate = item.strip()
            if candidate:
                normalised.append(candidate)
    return normalised


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Docker-backed benchmark smoke loops."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    smoke = subparsers.add_parser(
        "docker-smoke",
        help="Build the benchmark image optionally, then run sequential dummy smoke benchmarks.",
    )
    smoke.add_argument("--build", action="store_true")
    smoke.add_argument("--image", default=DEFAULT_BENCHMARK_IMAGE)
    smoke.add_argument("--dockerfile", default=DEFAULT_DOCKERFILE)
    smoke.add_argument("--data-dir", default="data")
    smoke.add_argument(
        "--engine",
        action="append",
        dest="engines",
        help="Repeatable engine filter. Defaults to the full shortlist.",
    )
    smoke.add_argument(
        "--workload-id",
        action="append",
        dest="workloads",
        help=(
            "Repeatable or comma-separated workload filter. "
            "Example: --workload-id BM_06,BM_07. "
            "Defaults to BM_06 for the initial smoke loop."
        ),
    )
    smoke.add_argument("--attempts", type=int, default=3)
    smoke.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress successful Docker build/run logs and show only minimal progress plus the final summary.",
    )
    smoke.add_argument("--timeout-seconds", type=int, default=900)
    smoke.add_argument("--results-dir")

    return parser


def run_subprocess(
    cmd: list[str],
    *,
    quiet: bool = False,
    failure_label: str = "command",
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        cmd,
        check=False,
        capture_output=quiet,
        text=True,
    )
    if completed.returncode != 0:
        if quiet:
            stdout = (completed.stdout or "").strip()
            stderr = (completed.stderr or "").strip()
            if stdout:
                print(stdout)
            if stderr:
                print(stderr, file=sys.stderr)
            if not stdout and not stderr:
                print(
                    f"{failure_label} failed with exit code {completed.returncode}",
                    file=sys.stderr,
                )
        raise SystemExit(completed.returncode)
    return completed


def ensure_image(image: str, dockerfile: str, *, quiet: bool = False) -> None:
    cmd = ["docker", "build", "-t", image, "-f", dockerfile, "."]
    run_subprocess(cmd, quiet=quiet, failure_label="docker build")


def load_result(path: Path) -> BenchmarkResult:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return BenchmarkResult(
        workload_id=payload["workload_id"],
        engine=payload["engine"],
        dataset_profile=payload["dataset_profile"],
        correct=payload["correct"],
        exit_code=payload.get("exit_code", 0),
        failure_type=payload.get("failure_type"),
        elapsed_seconds=payload["elapsed_seconds"],
        workload_exec_seconds=payload.get("workload_exec_seconds"),
        validation_seconds=payload.get("validation_seconds"),
        cold_start_seconds=payload.get("cold_start_seconds"),
        peak_memory_mb=payload["peak_memory_mb"],
        tmp_peak_mb=payload.get("tmp_peak_mb"),
        rows_in=payload.get("rows_in"),
        rows_out=payload.get("rows_out"),
        cpu_user_seconds=payload.get("cpu_user_seconds"),
        cpu_pct=payload.get("cpu_pct"),
        image_size_mb=payload.get("image_size_mb"),
        delta_write_seconds=payload.get("delta_write_seconds"),
        delta_read_seconds=payload.get("delta_read_seconds"),
        delta_roundtrip_ok=payload["delta_roundtrip_ok"],
        output_file_count=payload.get("output_file_count"),
        notes=payload["notes"],
        selected=payload["selected"],
    )


def inspect_image_size_mb(image: str) -> float | None:
    completed = subprocess.run(
        ["docker", "image", "inspect", image, "--format", "{{.Size}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None
    try:
        size_bytes = int(completed.stdout.strip())
    except ValueError:
        return None
    return size_bytes / (1024 * 1024)


def parse_cpu_percent(value: str) -> float | None:
    cleaned = value.strip().rstrip("%")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_memory_usage_mb(value: str) -> float | None:
    if "/" in value:
        value = value.split("/", 1)[0]
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([KMGTP]i?B|B)", value.strip())
    if not match:
        return None

    amount = float(match.group(1))
    unit = match.group(2)
    factors = {
        "B": 1.0 / (1024 * 1024),
        "KB": 1.0 / 1024,
        "KiB": 1.0 / 1024,
        "MB": 1.0,
        "MiB": 1.0,
        "GB": 1024.0,
        "GiB": 1024.0,
        "TB": 1024.0 * 1024.0,
        "TiB": 1024.0 * 1024.0,
        "PB": 1024.0 * 1024.0 * 1024.0,
        "PiB": 1024.0 * 1024.0 * 1024.0,
    }
    return amount * factors[unit]


def sample_container_stats(container_id: str) -> tuple[float | None, float | None]:
    completed = subprocess.run(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.CPUPerc}}|{{.MemUsage}}",
            container_id,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return None, None

    line = completed.stdout.strip()
    if not line or "|" not in line:
        return None, None

    cpu_raw, mem_raw = line.split("|", 1)
    return parse_cpu_percent(cpu_raw), parse_memory_usage_mb(mem_raw)


def classify_failure(return_code: int, *, timed_out: bool) -> str:
    if timed_out or return_code == 124:
        return "timeout"
    if return_code == 137:
        return "oom_killed"
    if return_code != 0:
        return "container_error"
    return "unknown"


def build_failure_result(
    *,
    workload_id: str,
    engine: str,
    dataset_profile: str,
    exit_code: int,
    failure_type: str,
    elapsed_seconds: float,
    image_size_mb: float | None,
    notes: str,
) -> BenchmarkResult:
    return BenchmarkResult(
        workload_id=workload_id,
        engine=engine,
        dataset_profile=dataset_profile,
        correct=False,
        exit_code=exit_code,
        failure_type=failure_type,
        elapsed_seconds=elapsed_seconds,
        workload_exec_seconds=None,
        validation_seconds=None,
        cold_start_seconds=None,
        peak_memory_mb=None,
        tmp_peak_mb=None,
        rows_in=None,
        rows_out=None,
        cpu_user_seconds=None,
        cpu_pct=None,
        image_size_mb=image_size_mb,
        delta_write_seconds=None,
        delta_read_seconds=None,
        delta_roundtrip_ok=False,
        output_file_count=None,
        notes=notes,
        selected=False,
    )


def summarise_results(results: list[BenchmarkResult]) -> str:
    groups: dict[tuple[str, str], list[BenchmarkResult]] = {}
    for result in results:
        groups.setdefault((result.engine, result.workload_id), []).append(result)

    headers = (
        "Engine",
        "Workload",
        "Attempts",
        "Successes",
        "MeanAll(ms)",
        "Work(ms)",
        "Cold(ms)",
        "StdDev(ms)",
        "Min",
        "Max",
        "PeakMemMB",
        "RowsIn",
        "RowsOut",
        "CPU(s)",
        "CPU%",
        "ImageMB",
        "DeltaW(ms)",
        "DeltaR(ms)",
        "Correct",
        "DeltaRT",
        "FailType",
    )
    rows = [headers]

    for (engine, workload_id), items in groups.items():
        elapsed_ms = [item.elapsed_seconds * 1000.0 for item in items]
        work_ms = [
            item.workload_exec_seconds * 1000.0
            for item in items
            if item.workload_exec_seconds is not None
        ]
        cold_ms = [
            item.cold_start_seconds * 1000.0
            for item in items
            if item.cold_start_seconds is not None
        ]
        peak_mem = [
            item.peak_memory_mb for item in items if item.peak_memory_mb is not None
        ]
        rows_in = [item.rows_in for item in items if item.rows_in is not None]
        rows_out = [item.rows_out for item in items if item.rows_out is not None]
        cpu_seconds = [
            item.cpu_user_seconds
            for item in items
            if item.cpu_user_seconds is not None
        ]
        cpu_pct = [item.cpu_pct for item in items if item.cpu_pct is not None]
        image_mb = [item.image_size_mb for item in items if item.image_size_mb is not None]
        delta_write_ms = [
            item.delta_write_seconds * 1000.0
            for item in items
            if item.delta_write_seconds is not None
        ]
        delta_read_ms = [
            item.delta_read_seconds * 1000.0
            for item in items
            if item.delta_read_seconds is not None
        ]
        failure_types = sorted(
            {item.failure_type for item in items if item.failure_type is not None}
        )

        def avg(values: list[float | int], digits: int = 1) -> str:
            if not values:
                return "-"
            return f"{statistics.fmean(values):.{digits}f}"

        rows.append(
            (
                engine,
                workload_id,
                str(len(items)),
                str(sum(1 for item in items if item.correct)),
                f"{statistics.fmean(elapsed_ms):.1f}",
                avg(work_ms),
                avg(cold_ms),
                (
                    f"{statistics.pstdev(elapsed_ms):.1f}"
                    if len(elapsed_ms) > 1
                    else "0.0"
                ),
                f"{min(elapsed_ms):.0f}",
                f"{max(elapsed_ms):.0f}",
                avg(peak_mem),
                avg(rows_in, digits=0),
                avg(rows_out, digits=0),
                avg(cpu_seconds, digits=4),
                avg(cpu_pct),
                avg(image_mb),
                avg(delta_write_ms),
                avg(delta_read_ms),
                "Y" if all(item.correct for item in items) else "N",
                "Y" if all(item.delta_roundtrip_ok for item in items) else "N",
                ",".join(failure_types) if failure_types else "-",
            )
        )

    widths = [
        max(len(str(row[index])) for row in rows) for index in range(len(headers))
    ]
    lines = []
    for row_index, row in enumerate(rows):
        parts = [
            str(value).ljust(widths[index]) for index, value in enumerate(row)
        ]
        lines.append("  ".join(parts))
        if row_index == 0:
            lines.append("  ".join("-" * width for width in widths))
    return "\n".join(lines)


def run_docker_smoke(args: argparse.Namespace) -> int:
    engines = normalise_csv_args(args.engines) or list(SHORTLIST_ENGINES)
    workloads = normalise_csv_args(args.workloads) or [DEFAULT_SMOKE_WORKLOAD]
    manifest = {item.id: item for item in filter_manifest(stage="stage1")}

    for engine in engines:
        if engine not in SHORTLIST_ENGINES:
            raise SystemExit(f"unsupported engine: {engine}")
    for workload_id in workloads:
        if workload_id not in manifest:
            raise SystemExit(f"unknown workload id: {workload_id}")
        workload = manifest[workload_id]
        if workload.candidate_engines and any(
            engine not in workload.candidate_engines for engine in engines
        ):
            supported = ", ".join(workload.candidate_engines)
            raise SystemExit(
                f"workload {workload_id} only supports engines: {supported}"
            )

    if args.build:
        if args.quiet:
            print(f"Building benchmark image {args.image}...", flush=True)
        ensure_image(args.image, args.dockerfile, quiet=args.quiet)
    image_size_mb = inspect_image_size_mb(args.image)

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        raise SystemExit(f"data directory does not exist: {data_dir}")

    if args.results_dir:
        results_dir = Path(args.results_dir).resolve()
        results_dir.mkdir(parents=True, exist_ok=True)
    else:
        results_dir = Path(tempfile.mkdtemp(prefix="benchmark_smoke_"))

    results: list[BenchmarkResult] = []

    if args.quiet:
        print(
            "Running benchmark smoke matrix...",
            flush=True,
        )

    for engine in engines:
        for workload_id in workloads:
            for attempt in range(1, args.attempts + 1):
                result_name = f"{engine}__{workload_id}__attempt_{attempt}.json"
                result_path = results_dir / result_name
                cidfile = results_dir / f"{engine}__{workload_id}__attempt_{attempt}.cid"
                cmd = [
                    "docker",
                    "run",
                    "--rm",
                    "--cidfile",
                    str(cidfile),
                    "--network=none",
                    "--memory=2g",
                    "--memory-swap=2g",
                    "--cpus=2",
                    "--read-only",
                    "--tmpfs",
                    "/tmp:rw,exec,size=512m",
                    "-e",
                    "PYTHONDONTWRITEBYTECODE=1",
                    "-v",
                    f"{data_dir}:/benchmarks/data:ro",
                    "-v",
                    f"{results_dir}:/benchmarks/results:rw",
                    args.image,
                    "python",
                    "-m",
                    "benchmarks.container_entrypoint",
                    "--engine",
                    engine,
                    "--workload-id",
                    workload_id,
                    "--dataset-profile",
                    "docker_smoke",
                    "--attempt",
                    str(attempt),
                    "--result-path",
                    f"/benchmarks/results/{result_name}",
                ]
                start = time.perf_counter()
                timed_out = False
                cpu_samples: list[float] = []
                mem_samples: list[float] = []
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                try:
                    container_id: str | None = None
                    while True:
                        return_code = process.poll()
                        if return_code is not None:
                            break

                        elapsed_so_far = time.perf_counter() - start
                        if elapsed_so_far > args.timeout_seconds:
                            timed_out = True
                            if cidfile.exists():
                                container_id = cidfile.read_text(encoding="utf-8").strip()
                                subprocess.run(
                                    ["docker", "rm", "-f", container_id],
                                    check=False,
                                    capture_output=True,
                                    text=True,
                                )
                            process.kill()
                            break

                        if container_id is None and cidfile.exists():
                            container_id = cidfile.read_text(encoding="utf-8").strip()

                        if container_id:
                            cpu_pct, mem_mb = sample_container_stats(container_id)
                            if cpu_pct is not None:
                                cpu_samples.append(cpu_pct)
                            if mem_mb is not None:
                                mem_samples.append(mem_mb)

                        time.sleep(STATS_POLL_SECONDS)

                    stdout, stderr = process.communicate(timeout=5)
                    completed = subprocess.CompletedProcess(
                        process.args,
                        process.returncode,
                        stdout,
                        stderr,
                    )
                except subprocess.TimeoutExpired:
                    timed_out = True
                    completed = None
                elapsed_seconds = time.perf_counter() - start
                if cidfile.exists():
                    cidfile.unlink(missing_ok=True)

                if timed_out:
                    results.append(
                        build_failure_result(
                            workload_id=workload_id,
                            engine=engine,
                            dataset_profile="docker_smoke",
                            exit_code=124,
                            failure_type="timeout",
                            elapsed_seconds=elapsed_seconds,
                            image_size_mb=image_size_mb,
                            notes="docker run exceeded timeout_seconds",
                        )
                    )
                    continue

                assert completed is not None
                if completed.returncode == 0 and result_path.exists():
                    result = load_result(result_path)
                    avg_cpu_pct = (
                        statistics.fmean(cpu_samples) if cpu_samples else result.cpu_pct
                    )
                    peak_mem_mb = max(mem_samples) if mem_samples else result.peak_memory_mb
                    cpu_seconds = (
                        elapsed_seconds * (avg_cpu_pct / 100.0)
                        if avg_cpu_pct is not None
                        else result.cpu_user_seconds
                    )
                    validation_seconds = result.validation_seconds or 0.0
                    cold_start_seconds = (
                        elapsed_seconds
                        - result.workload_exec_seconds
                        - validation_seconds
                        if result.workload_exec_seconds is not None
                        else None
                    )
                    final_result = replace(
                        result,
                        exit_code=0,
                        failure_type=None,
                        elapsed_seconds=elapsed_seconds,
                        validation_seconds=result.validation_seconds,
                        cold_start_seconds=cold_start_seconds,
                        peak_memory_mb=peak_mem_mb,
                        cpu_user_seconds=cpu_seconds,
                        cpu_pct=avg_cpu_pct,
                        image_size_mb=image_size_mb,
                    )
                    result_payload = json.dumps(
                        final_result.as_dict(),
                        indent=2,
                        sort_keys=True,
                    )
                    try:
                        result_path.write_text(
                            result_payload,
                            encoding="utf-8",
                        )
                    except PermissionError:
                        result_path.unlink(missing_ok=True)
                        result_path.write_text(
                            result_payload,
                            encoding="utf-8",
                        )
                    results.append(final_result)
                    continue

                stderr = (completed.stderr or "").strip()
                stdout = (completed.stdout or "").strip()
                notes = stderr or stdout or "docker run returned non-zero exit code"
                results.append(
                    build_failure_result(
                        workload_id=workload_id,
                        engine=engine,
                        dataset_profile="docker_smoke",
                        exit_code=completed.returncode,
                        failure_type=classify_failure(
                            completed.returncode,
                            timed_out=False,
                        ),
                        elapsed_seconds=elapsed_seconds,
                        image_size_mb=image_size_mb,
                        notes=notes[:500],
                    )
                )

    print(summarise_results(results))
    print("")
    print(f"Results directory: {results_dir}")

    return 0


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "docker-smoke":
        return run_docker_smoke(args)
    raise SystemExit(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
