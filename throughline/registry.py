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
        globally — in either case Throughline cannot vouch for it.
        """
        return bool(getattr(self.func, "__throughline__", {}).get("replay_safe"))


_PLANS: dict[str, list[Step]] = {}


def register_replay(dag_id: str, steps: list[tuple]) -> None:
    """Declare the ordered steps that a replay of ``dag_id`` should run."""
    _PLANS[dag_id] = [Step(*s) for s in steps]


#: Which run types a given DAG captures. Absent means "use the defaults".
_POLICIES: dict[str, frozenset[str]] = {}


def trace_policy(dag_id: str, run_types: set[str] | frozenset[str]) -> None:
    """Declare which run types capture for ``dag_id``.

    The per-run switches are the right tool for one run, and the deployment
    environment variable is the right tool for a fleet, but neither answers
    "this DAG records its automatic runs and that one does not". This does,
    in one line per DAG:

        throughline.trace_policy("orders_enrichment", {"manual", "scheduled"})

    Airflow 3.1's run types are ``manual``, ``scheduled``, ``backfill`` and
    ``asset_triggered``. A DAG that says nothing keeps the defaults, so this
    is additive: no existing DAG changes behaviour by its introduction.

    An explicit answer still outranks it — an unticked Trigger checkbox or
    ``{"throughline": {"trace": false}}`` turns a run off whatever the policy
    says, because a policy is a default and those are instructions.
    """
    _POLICIES[dag_id] = frozenset(t.lower() for t in run_types)


def policy(dag_id: str | None) -> frozenset[str] | None:
    """The run types ``dag_id`` captures, or ``None`` if it never said."""
    return _POLICIES.get(dag_id) if dag_id else None


def plan(dag_id: str) -> list[Step]:
    return list(_PLANS.get(dag_id, []))


def registered() -> list[str]:
    return sorted(_PLANS)


def unsafe_steps(dag_id: str) -> list[str]:
    """Tasks blocking a replay, by name, so the refusal can say which."""
    return [s.task_id for s in plan(dag_id) if not s.replay_safe]
