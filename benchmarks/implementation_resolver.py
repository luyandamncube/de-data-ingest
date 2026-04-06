"""Resolve hybrid workload implementations for a selected engine."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pipeline.registry import TrackingUnit


@dataclass(frozen=True, slots=True)
class ResolvedImplementation:
    kind: str
    ref: str
    engine: str
    callable_obj: Callable | None = None
    sql_path: str | None = None


def resolve_implementation(
    workload: TrackingUnit,
    *,
    engine: str,
) -> ResolvedImplementation:
    implementations = workload.implementations

    if engine in implementations.engine_python_refs:
        ref = implementations.engine_python_refs[engine]
        return ResolvedImplementation(
            kind="python",
            ref=ref,
            engine=engine,
            callable_obj=load_python_callable(ref, workload.id),
        )

    if engine in implementations.engine_sql_refs:
        ref = implementations.engine_sql_refs[engine]
        return ResolvedImplementation(
            kind="sql",
            ref=ref,
            engine=engine,
            sql_path=load_sql_path(ref, workload.id),
        )

    if implementations.common_python_ref:
        ref = implementations.common_python_ref
        return ResolvedImplementation(
            kind="python",
            ref=ref,
            engine=engine,
            callable_obj=load_python_callable(ref, workload.id),
        )

    if implementations.common_sql_ref:
        ref = implementations.common_sql_ref
        return ResolvedImplementation(
            kind="sql",
            ref=ref,
            engine=engine,
            sql_path=load_sql_path(ref, workload.id),
        )

    raise SystemExit(
        f"workload {workload.id} has no implementation registered for engine {engine}"
    )


def load_python_callable(ref: str, workload_id: str) -> Callable:
    module_path, sep, callable_name = ref.partition(":")
    if not sep or not callable_name:
        raise SystemExit(
            f"invalid python implementation ref for workload {workload_id}: {ref}"
        )

    module = importlib.import_module(module_path)
    try:
        return getattr(module, callable_name)
    except AttributeError as exc:
        raise SystemExit(
            f"implementation callable not found for workload {workload_id}: {ref}"
        ) from exc


def load_sql_path(ref: str, workload_id: str) -> str:
    path = Path(ref)
    if not path.exists():
        raise SystemExit(
            f"sql implementation path does not exist for workload {workload_id}: {ref}"
        )
    return str(path)
