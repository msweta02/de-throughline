"""Re-running one record through real task code, safely.

Two guarantees, in order of how much they matter:

1. **It cannot write to production.** The warehouse is attached ``READ_ONLY``
   and a scratch database takes the writes (see :mod:`passage.session`). A task
   with a hardcoded production write raises instead of succeeding quietly. This
   is enforced by the database, not by every task remembering a convention.
2. **Tasks opt in.** A DAG with tasks nobody marked ``replay_safe`` can still
   be traced, but refuses to replay, naming the tasks that are not marked.

Refusing is the right default. The cost of refusing is an error message; the
cost of not refusing is a corrupted production table.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from passage import errors, paths, registry, runtime, store


@dataclass
class ReplayResult:
    replay_id: str
    dag_id: str
    scope: str
    bundle_version: str
    status: str
    record_key: str | None = None
    note: str | None = None
    tasks: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status == "ok"


def preflight(dag_id: str) -> list[str]:
    """Reasons this DAG cannot be replayed. Empty means it can."""
    if not registry.plan(dag_id):
        return [f"no replay plan registered for {dag_id!r}"]
    unsafe = registry.unsafe_steps(dag_id)
    if unsafe:
        return [
            "these tasks are not marked replay_safe, so Passage will not "
            f"re-execute them: {', '.join(unsafe)}"
        ]
    return []


def run(
    dag_id: str,
    scope: str,
    bundle_version: str = "current",
    record_key: str | None = None,
) -> ReplayResult:
    """Replay one record through ``dag_id`` and capture every step."""
    replay_id = f"replay__{uuid.uuid4().hex[:10]}"

    problems = preflight(dag_id)
    if problems:
        result = ReplayResult(replay_id, dag_id, scope, bundle_version,
                              "refused", record_key, "; ".join(problems))
        _save(result)
        raise errors.ReplayRefused(result.note or "replay refused")

    conf = {
        "passage": {
            "trace": True,
            "replay": True,
            "replay_id": replay_id,
            "scope": scope,
        }
    }
    base = dict(dag_id=dag_id, run_id=replay_id, bundle_version=bundle_version,
                run_type="manual", conf=conf)

    previous: Any = None
    ran: list[str] = []
    saved = runtime.set_override()
    try:
        for step in registry.plan(dag_id):
            runtime.set_override(task_id=step.task_id, **base)
            if step.arg == "scope":
                previous = step.func(scope)
            elif step.arg == "none":
                previous = step.func()
            else:
                previous = step.func(previous)
            ran.append(step.task_id)
    except Exception as exc:  # noqa: BLE001 - reported, not swallowed
        result = ReplayResult(replay_id, dag_id, scope, bundle_version, "failed",
                              record_key, f"{type(exc).__name__}: {exc}", ran)
        _save(result)
        return result
    finally:
        runtime.clear_override(saved)

    # The replay captured under its own run_id, so the record it produced is
    # whatever key came out of it.
    if record_key is None:
        records = store.list_records(dag_id, replay_id, limit=1)
        record_key = str(records[0]["record_key"]) if records else None

    result = ReplayResult(replay_id, dag_id, scope, bundle_version, "ok",
                          record_key, None, ran)
    _save(result)
    return result


def _save(result: ReplayResult) -> None:
    store.save_replay(
        {
            "replay_id": result.replay_id,
            "dag_id": result.dag_id,
            "source_run_id": None,
            "bundle_version": result.bundle_version,
            "record_key": result.record_key,
            "scope": result.scope,
            "query": None,
            "status": result.status,
            "note": result.note,
            "is_regression": False,
        }
    )


def discard_scratch(replay_id: str) -> None:
    """Delete a replay's scratch database. The captures outlive it."""
    path = paths.scratch_db(replay_id)
    if path.exists():
        path.unlink()
