"""The three independent switches, all defaulting to off.

Capture has to cost nothing when nobody asked for it, or nobody will allow it
near a real deployment. "Nothing" here means *no wrapper in the call path*, not
an early ``return`` inside one:

1. **Global** — an Airflow Variable, read once at DAG-parse time. When it is
   off, ``@passage.trace`` hands back the undecorated function and Passage is
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
GLOBAL_SWITCH = "passage_enabled"

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
    if "PASSAGE_ENABLED" in os.environ:
        return _as_bool(os.environ["PASSAGE_ENABLED"])
    try:
        from airflow.sdk import Variable

        return _as_bool(Variable.get(GLOBAL_SWITCH, default=None))
    except Exception:
        # No Airflow, no Variable, or no database. All mean "off".
        return False


def run_enabled(run_type: str, passage_conf: dict) -> bool:
    """Switch 3, evaluated inside the task.

    ``{"passage": {"trace": true}}`` in ``dag_run.conf`` turns capture on for a
    single run; ``{"passage": {"trace": false}}`` turns it off even for a
    manual one. With nothing said, manual runs and replays capture and
    scheduled runs do not — a scheduled production run is exactly the place
    where an unasked-for side effect is least welcome.
    """
    if "trace" in passage_conf:
        return _as_bool(passage_conf["trace"])
    if passage_conf.get("replay"):
        return True
    return run_type.lower() in {"manual", "manual_triggered", "backfill"}


def sample_records(passage_conf: dict) -> int | None:
    """How many distinct records to capture. ``None`` means every one.

    A scoped replay is one record by construction, so the cap is lifted there
    rather than having to be raised by hand.
    """
    if passage_conf.get("scope"):
        return None
    if "sample" in passage_conf:
        value = passage_conf["sample"]
        if value in (None, "all", 0):
            return None
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            pass
    return int(os.environ.get("PASSAGE_SAMPLE_RECORDS", DEFAULT_SAMPLE_RECORDS))
