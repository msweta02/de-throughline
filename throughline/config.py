"""The three independent switches, all defaulting to off.

Capture has to cost nothing when nobody asked for it, or nobody will allow it
near a real deployment. "Nothing" here means *no wrapper in the call path*, not
an early ``return`` inside one:

1. **Global** — an Airflow Variable, read once at DAG-parse time. When it is
   off, ``@throughline.trace`` hands back the undecorated function and Throughline is
   not in the call stack at all.
2. **Per task** — whether the decorator was applied to that task.
3. **Per run** — ``dag_run.conf``. Scheduled runs capture nothing by default;
   manual runs and replays capture.

Plus a sampling cap, so a traced run over a large table records the first N
records rather than all of them.
"""

from __future__ import annotations

import os

#: Airflow Variable consulted at parse time. Absent means off.
GLOBAL_SWITCH = "throughline_enabled"

#: Distinct records captured per snapshot when the run has not scoped itself.
DEFAULT_SAMPLE_RECORDS = 100

_TRUTHY = {"1", "true", "t", "yes", "y", "on"}


def _as_bool(value: object) -> bool:
    return str(value).strip().lower() in _TRUTHY if value is not None else False


def globally_enabled() -> bool:
    """Switch 1, evaluated when the DAG file is parsed.

    Reading an Airflow Variable at parse time is ordinarily an anti-pattern —
    it is a database round trip on every parse. It is the deliberate choice
    here because the alternative (checking at run time) means the wrapper is
    always in the call path, which is the cost this switch exists to avoid.
    The environment variable is checked first so local runs, tests and CI never
    touch the metadata database at all.
    """
    if "THROUGHLINE_ENABLED" in os.environ:
        return _as_bool(os.environ["THROUGHLINE_ENABLED"])
    return _as_bool(_read_switch())


def _read_switch() -> object | None:
    """The switch Variable, read through whichever accessor works here.

    ``airflow.sdk.Variable`` only works *inside a running task*: at DAG-parse
    time it raises ``ImportError`` on ``SUPERVISOR_COMMS``. Since this function
    runs at parse time, that made it return False and remove the decorator
    everywhere — setting the Variable had no effect at all. The metadata-DB
    accessor is the one that works at parse time, so it is tried first, and the
    Task SDK one still covers a worker with no database access.

    Either way a failure means "off", because a tracing tool that breaks a
    parse because it could not read a Variable has failed at its job.
    """
    try:
        from airflow.models import Variable

        return Variable.get(GLOBAL_SWITCH, default_var=None)
    except Exception:
        pass
    try:
        from airflow.sdk import Variable as SdkVariable

        return SdkVariable.get(GLOBAL_SWITCH, default=None)
    except Exception:
        # No Airflow, no Variable, or no database. All mean "off".
        return None


#: A DAG may declare this as a ``Param``, which makes Airflow's own Trigger
#: dialog render a checkbox for it. Params are only written into
#: ``dag_run.conf`` when a run is actually triggered with them, so a scheduled
#: run's conf stays empty and the run-type default below still governs it.
TRACE_PARAM = "throughline_trace"

#: Run types that capture when nothing else has decided. Scheduled runs are
#: absent on purpose: a scheduled production run is where an unasked-for side
#: effect is least welcome, and somebody has to have asked for it.
_TRACED_RUN_TYPES = frozenset({"manual", "manual_triggered", "backfill"})

#: Opt in to tracing scheduled runs too. Off unless set, and an environment
#: variable rather than an Airflow Variable because this is read *inside every
#: task*, where a metadata-database round trip per task would be a real cost.
TRACE_SCHEDULED = "THROUGHLINE_TRACE_SCHEDULED"


def _traced_run_types() -> frozenset[str]:
    """Which run types capture by default, widened if the deployment asked."""
    if _as_bool(os.environ.get(TRACE_SCHEDULED)):
        return _TRACED_RUN_TYPES | {"scheduled"}
    return _TRACED_RUN_TYPES


def run_enabled(run_type: str, throughline_conf: dict, conf: dict | None = None) -> bool:
    """Switch 3, evaluated inside the task.

    Four ways to decide, most explicit first:

    1. ``{"throughline": {"trace": true|false}}`` in ``dag_run.conf`` — the
       programmatic form, and the only one that can force capture *off* for a
       replay.
    2. A replay always captures; that is the point of running one.
    3. ``throughline_trace`` at the top level of ``dag_run.conf``, which is
       where Airflow puts the checkbox from the Trigger dialog when a DAG
       declares the matching ``Param``.
    4. Otherwise: manual runs capture and scheduled runs do not, because a
       scheduled production run is exactly the place where an unasked-for
       side effect is least welcome.

    The checkbox sits below the replay check on purpose. A replay carries no
    params, but if one ever did, an unticked box must not be able to turn a
    replay into a run that records nothing.
    """
    if "trace" in throughline_conf:
        return _as_bool(throughline_conf["trace"])
    if throughline_conf.get("replay"):
        return True
    if conf and TRACE_PARAM in conf:
        return _as_bool(conf[TRACE_PARAM])
    return run_type.lower() in _traced_run_types()


def sample_records(throughline_conf: dict) -> int | None:
    """How many distinct records to capture. ``None`` means every one.

    A scoped replay is one record by construction, so the cap is lifted there
    rather than having to be raised by hand.
    """
    if throughline_conf.get("scope"):
        return None
    if "sample" in throughline_conf:
        value = throughline_conf["sample"]
        if value in (None, "all", 0):
            return None
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            pass

    # Read defensively. This runs inside the task, and a typo in an environment
    # variable must not be able to fail somebody's pipeline.
    try:
        return max(1, int(os.environ.get("THROUGHLINE_SAMPLE_RECORDS", DEFAULT_SAMPLE_RECORDS)))
    except (TypeError, ValueError):
        return DEFAULT_SAMPLE_RECORDS
