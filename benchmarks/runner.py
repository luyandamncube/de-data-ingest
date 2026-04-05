"""CLI skeleton for interacting with the benchmark registry."""

from __future__ import annotations

import argparse
import json

from benchmarks.results import BenchmarkMatrix, build_sample_result
from pipeline.registry import TrackingRole, filter_manifest, manifest_as_dicts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark-suite skeleton for listing tracked units and "
        "emitting sample results."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser(
        "list", help="List tracked units from the canonical manifest."
    )
    list_parser.add_argument("--stage", help="Optional stage filter, e.g. stage1")
    list_parser.add_argument(
        "--role",
        choices=[role.value for role in TrackingRole],
        help="Optional role filter.",
    )

    sample_parser = subparsers.add_parser(
        "sample-result", help="Emit a sample benchmark matrix result."
    )
    sample_parser.add_argument("--workload-id", required=True)
    sample_parser.add_argument("--engine", default="pyspark_delta")
    sample_parser.add_argument("--dataset-profile", default="smoke")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "list":
        role = TrackingRole(args.role) if args.role else None
        print(
            json.dumps(
                manifest_as_dicts(role=role, stage=args.stage),
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    workload_ids = {item.id for item in filter_manifest()}
    if args.workload_id not in workload_ids:
        parser.error(f"unknown workload id: {args.workload_id}")

    matrix = BenchmarkMatrix()
    matrix.add_result(
        build_sample_result(
            args.workload_id,
            args.engine,
            dataset_profile=args.dataset_profile,
        )
    )
    print(matrix.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
