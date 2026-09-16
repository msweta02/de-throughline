"""Everything Passage needs to know about the task it is currently inside.

This is the only module that talks to Airflow. It is deliberately the only one,
because Airflow 3.1 is recent and these accessors are the easiest thing in the
project to get wrong: concentrating them here means a wrong guess is a one-file
fix, and means every other module can be exercised without Airflow installed.

Every accessor degrades instead of raising. A tracing tool that breaks a task
because it could not read ``bundle_version`` is worse than one that records the
version as ``unknown``.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

# Set by the replay runner in-process; see ``passage.replay``. Local runs and
# tests use this too, which is why it is a plain module global rather than
# anything Airflow-specific.
_OVERRIDE: dict[str, Any] = {}


def set_override(**kwargs: Any) -> dict[str, Any]:
    """Force the runtime facts. Returns the previous values, for restoring."""
    previous = dict(_OVERRIDE)
    _OVERRIDE.update(kwargs)
    return previous


def clear_override(previous: dict[str, Any] | None = None) -> None:
    _OVERRIDE.clear()
    if previous:
        _OVERRIDE.update(previous)


def _airflow_context() -> dict[str, Any] | None:
    """The live task context, or ``None`` when we are not inside a task."""
    try:
        from airflow.sdk import get_current_context
    except Exception:  # pragma: no cover - Airflow absent (tests, tooling)
        return None
    try:
        return dict(get_current_context())
    except Exception:
        # Raised when called outside an execution context. Not an error here.
        return None


def _bundle_version(context: dict[str, Any] | None) -> str:
    """Which version of the DAG bundle is running.

    UNVERIFIED against a live Airflow 3.1 deployment — see VERIFY.md. The
    attribute is tried first and every fallback is silent by design, so a rename
    upstream costs the diff view its labels, not the run.
    """
    if context:
        dag_run = context.get("dag_run")
        for attr in ("bundle_version", "dag_version_name"):
            value = getattr(dag_run, attr, None)
            if value:
                return str(value)
    return os.environ.get("PASSAGE_BUNDLE_VERSION", "unknown")


@dataclass(frozen=True)
class Runtime:
    """The facts a capture is keyed by."""

    dag_id: str
    run_id: str
    task_id: str
    bundle_version: str
    run_type: str
    conf: dict[str, Any] = field(default_factory=dict)

    @property
    def passage_conf(self) -> dict[str, Any]:
        """The ``passage`` block of ``dag_run.conf``, if the caller set one."""
        block = self.conf.get("passage")
        return block if isinstance(block, dict) else {}

    @property
    def is_replay(self) -> bool:
        return bool(self.passage_conf.get("replay"))

    @property
    def replay_id(self) -> str | None:
        value = self.passage_conf.get("replay_id")
        return str(value) if value else None

    @property
    def scope(self) -> str | None:
        """The predicate a scoped replay narrows source queries with."""
        value = self.passage_conf.get("scope")
        return str(value) if value else None


def current() -> Runtime:
    """Resolve the current runtime, from Airflow if present and env if not."""
    context = _airflow_context()
    dag_run = context.get("dag_run") if context else None

    conf: dict[str, Any] = {}
    if dag_run is not None and isinstance(getattr(dag_run, "conf", None), dict):
        conf = dict(dag_run.conf)
    elif os.environ.get("PASSAGE_CONF"):
        try:
            conf = json.loads(os.environ["PASSAGE_CONF"])
        except json.JSONDecodeError:
            conf = {}

    task_instance = (context or {}).get("ti")

    resolved = Runtime(
        dag_id=str(getattr(task_instance, "dag_id", None) or os.environ.get("PASSAGE_DAG_ID", "unknown")),
        run_id=str((context or {}).get("run_id") or os.environ.get("PASSAGE_RUN_ID", "local")),
        task_id=str(getattr(task_instance, "task_id", None) or os.environ.get("PASSAGE_TASK_ID", "unknown")),
        bundle_version=_bundle_version(context),
        run_type=str(getattr(dag_run, "run_type", None) or os.environ.get("PASSAGE_RUN_TYPE", "manual")),
        conf=conf,
    )

    if _OVERRIDE:
        return Runtime(**{**resolved.__dict__, **_OVERRIDE})
    return resolved
