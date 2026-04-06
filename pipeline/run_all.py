"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

import os
from pathlib import Path

from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


def _align_runtime_identity(output_root: str = "/data/output") -> None:
    """Drop to the mounted output directory owner when running as root.

    The local harness mounts /data/output as a user-owned temp directory while
    also dropping Linux capabilities. Matching the mount owner keeps writes
    working under those restricted container flags.
    """

    if os.getuid() != 0:
        return

    output_path = Path(output_root)
    try:
        stat_result = output_path.stat()
    except OSError:
        return

    target_uid = stat_result.st_uid
    target_gid = stat_result.st_gid
    if target_uid == 0 and target_gid == 0:
        return

    os.environ.setdefault("HOME", "/tmp")
    os.environ.setdefault("USER", str(target_uid))
    try:
        os.setgroups([])
        os.setgid(target_gid)
        os.setuid(target_uid)
    except PermissionError:
        return


if __name__ == "__main__":
    _align_runtime_identity()
    run_ingestion()
    run_transformation()
    run_provisioning()
