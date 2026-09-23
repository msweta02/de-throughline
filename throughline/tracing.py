"""``@throughline.trace`` — the capture decorator.

The module is ``tracing`` and not ``trace`` so that ``throughline.trace`` is
unambiguously the decorator. With both named the same, ``from throughline import
trace`` would hand back the module or the function depending on import order,
which is exactly the kind of bug that shows up once and is never reproducible.

One line per task, and the task itself is untouched: same arguments, same
return value, same side effects. Throughline reads what went in and what came out
and writes it to its own store.

Decorator order matters, and not in the way you might first write it::

    @task
    @throughline.trace(key="order_id")
    def normalize(orders): ...

``@throughline.trace`` goes *below* ``@task``. Airflow's ``@task`` has to be the
outermost decorator, because it turns the function into something that builds a
task at DAG-parse time; wrapping *that* would run the capture wrapper while the
DAG file is being parsed rather than inside the worker. Applying trace first
means ``@task`` receives the already-wrapped function and executes it, wrapper
and all, at run time. Getting this backwards is silent enough to be worth a
loud error, so :class:`~throughline.errors.DecoratorOrderError` checks for it.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any, TypeVar

from throughline import config, errors, runtime, session, store
from throughline import snapshot as snapshot_mod
from throughline import table as table_mod

log = logging.getLogger("throughline")

F = TypeVar("F", bound=Callable[..., Any])


def _looks_like_airflow_task(obj: Any) -> bool:
    """Whether this is already an Airflow task rather than a plain function."""
    return all(hasattr(obj, attr) for attr in ("override", "function"))


def _pick_input(args: tuple, kwargs: dict) -> Any:
    """The argument that holds the records going in.

    The first argument that is record-shaped wins. In practice that is the
    upstream task's output — the handle or rows it returned — and everything
    else in the signature is configuration.
    """
    for value in list(args) + list(kwargs.values()):
        if table_mod.is_table(value):
            return value
        if isinstance(value, (list, tuple)) and value and all(isinstance(r, dict) for r in value):
            return value
        if callable(getattr(value, "to_dict", None)):
            return value
    return None


def _capture(
    rt: runtime.Runtime, direction: str, value: Any, key: str | None, sample: int | None
) -> None:
    """Snapshot one side and store it. Never raises."""
    try:
        con = None
        try:
            if table_mod.is_table(value):
                con = session.connect_observer(rt)
            snap = snapshot_mod.take(value, key=key, sample=sample, con=con)
        finally:
            if con is not None:
                con.close()
        if snap.captured:
            store.record(rt, direction, snap, key)
    except Exception as exc:  # noqa: BLE001
        # A tool that breaks the pipeline it is observing has failed at its job.
        log.warning("throughline: %s capture failed for %s: %s", direction, rt.task_id, exc)


def trace(
    func: F | None = None,
    *,
    key: str | None = None,
    replay_safe: bool = False,
    capture_input: bool = True,
    capture_output: bool = True,
) -> Any:
    """Capture what this task received and what it produced.

    Args:
        key: the field identifying a record, e.g. ``"order_id"``. Without it
            Throughline still records shape (row counts, fields added and dropped)
            but cannot follow one record across tasks.
        replay_safe: whether this task may be re-executed during a replay.
            Off by default: a replay runs the real task code, and that code
            contains writes.
        capture_input / capture_output: which sides to snapshot.
    """

    def decorate(target: F) -> F:
        if _looks_like_airflow_task(target):
            raise errors.DecoratorOrderError(
                "@throughline.trace must be applied below @task, not above it:\n"
                "    @task\n"
                "    @throughline.trace(key=...)\n"
                "    def my_task(...): ...\n"
                "Applied above @task, the wrapper would run at DAG-parse time "
                "instead of inside the worker."
            )

        # Switch 1, at parse time. When Throughline is off globally the undecorated
        # function is handed straight back: no wrapper, nothing in the call
        # path, nothing to cost anything at run time.
        if not config.globally_enabled():
            return target

        @functools.wraps(target)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rt = runtime.current()

            if rt.is_replay and not replay_safe:
                raise errors.ReplayRefused(
                    f"task {rt.task_id!r} is not marked replay-safe. Replay re-executes "
                    f"real task code; mark it @throughline.trace(replay_safe=True) once you "
                    f"have checked that its writes are parameterised."
                )

            # Switch 3, at run time.
            if not config.run_enabled(rt.run_type, rt.throughline_conf, rt.conf):
                return target(*args, **kwargs)

            sample = config.sample_records(rt.throughline_conf)

            if capture_input:
                _capture(rt, "in", _pick_input(args, kwargs), key, sample)

            result = target(*args, **kwargs)

            if capture_output:
                _capture(rt, "out", result, key, sample)

            return result

        # Lets a DAG-level preflight report which tasks refuse to replay before
        # a replay starts, rather than one task into it.
        wrapper.__throughline__ = {  # type: ignore[attr-defined]
            "key": key,
            "replay_safe": replay_safe,
            "task_name": getattr(target, "__name__", "?"),
        }
        return wrapper  # type: ignore[return-value]

    return decorate(func) if func is not None else decorate
