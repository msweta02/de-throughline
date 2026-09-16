"""Which callables make up a replayable DAG.

Replay re-executes real task code, so something has to know what that code is.
Airflow knows the task graph, but the API server process does not necessarily
have the DAG module imported, and reaching back through the scheduler to
re-run a whole DAG would cost the "runs in seconds" that makes replay useful.

So a DAG registers its replay plan: an ordered list of
``(task_id, callable, argument)``. It is a few lines per DAG and it is explicit,
which beats discovering callables by reflection and guessing how to chain them.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

#: How a step gets its input: the scope predicate, or the previous step's handle.
ArgKind = Literal["scope", "previous", "none"]


@dataclass(frozen=True)
class Step:
    task_id: str
    func: Callable
    arg: ArgKind = "previous"

    @property
    def replay_safe(self) -> bool:
        """Whether this task was explicitly marked safe to re-execute.

        Absent metadata counts as unsafe. That covers both a task nobody marked
        and a task whose decorator was compiled out because tracing is off
        globally — in either case Passage cannot vouch for it.
        """
        return bool(getattr(self.func, "__passage__", {}).get("replay_safe"))


_PLANS: dict[str, list[Step]] = {}


def register_replay(dag_id: str, steps: list[tuple]) -> None:
    """Declare the ordered steps that a replay of ``dag_id`` should run."""
    _PLANS[dag_id] = [Step(*s) for s in steps]


def plan(dag_id: str) -> list[Step]:
    return list(_PLANS.get(dag_id, []))


def registered() -> list[str]:
    return sorted(_PLANS)


def unsafe_steps(dag_id: str) -> list[str]:
    """Tasks blocking a replay, by name, so the refusal can say which."""
    return [s.task_id for s in plan(dag_id) if not s.replay_safe]
