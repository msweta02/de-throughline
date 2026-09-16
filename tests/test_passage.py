"""Sanity checks on the parts that would fail silently if they broke.

Deliberately few. The three switches, the decorator order, and row_ordinal are
the things whose failure would not be obvious from looking at the UI: capture
that quietly does not happen, or a fan-out that quietly collapses to one row.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture()
def capture_home(tmp_path, monkeypatch):
    """Point Passage at a throwaway store, with tracing on."""
    monkeypatch.setenv("PASSAGE_HOME", str(tmp_path))
    monkeypatch.setenv("PASSAGE_ENABLED", "1")
    yield tmp_path


def test_global_switch_off_leaves_no_wrapper(monkeypatch):
    """Off means the function is handed back untouched, not wrapped in a no-op."""
    monkeypatch.setenv("PASSAGE_ENABLED", "0")
    from passage import tracing

    def step(rows):
        return rows

    decorated = tracing.trace(key="order_id")(step)
    assert decorated is step
    assert not hasattr(decorated, "__passage__")


def test_global_switch_on_wraps(monkeypatch):
    monkeypatch.setenv("PASSAGE_ENABLED", "1")
    from passage import tracing

    decorated = tracing.trace(key="order_id", replay_safe=True)(lambda rows: rows)
    assert decorated.__passage__["replay_safe"] is True


def test_decorator_applied_above_task_is_rejected(monkeypatch):
    """@passage.trace above @task would run at parse time. That must be loud."""
    monkeypatch.setenv("PASSAGE_ENABLED", "1")
    from passage import errors, tracing

    class FakeAirflowTask:
        def override(self): ...
        def function(self): ...

    with pytest.raises(errors.DecoratorOrderError):
        tracing.trace(FakeAirflowTask())


def test_scheduled_runs_capture_nothing_by_default():
    from passage import config

    assert config.run_enabled("scheduled", {}) is False
    assert config.run_enabled("manual", {}) is True
    assert config.run_enabled("scheduled", {"trace": True}) is True
    assert config.run_enabled("manual", {"trace": False}) is False


def test_fan_out_is_recorded_as_two_ordinals(capture_home):
    """One record key, two rows. Collapsing these would hide the whole bug."""
    from passage import runtime, snapshot, store

    run_id = f"test__{uuid.uuid4().hex[:8]}"
    rt = runtime.Runtime("d", run_id, "apply_promo", "v1", "manual", {})
    snap = snapshot.take(
        [{"order_id": 1, "promo": "A"}, {"order_id": 1, "promo": "B"}], key="order_id"
    )
    store.record(rt, "out", snap, "order_id")

    cells = store.captures_for("d", run_id, "1")
    assert {c["row_ordinal"] for c in cells} == {0, 1}
    assert {c["value"] for c in cells if c["field_name"] == "promo"} == {"A", "B"}


def test_sampling_caps_by_record_not_by_row(capture_home):
    """A capped snapshot must still bring every row of the records it keeps."""
    from passage import snapshot

    rows = [{"order_id": 1, "n": 1}, {"order_id": 1, "n": 2}, {"order_id": 2, "n": 3}]
    kept = snapshot.take(rows, key="order_id", sample=1).rows
    assert kept == rows[:2]


def test_trace_returns_the_task_result_untouched(capture_home):
    """Capture is an observation. It must not alter what the task returns."""
    from passage import tracing

    sentinel = [{"order_id": 7, "a": 1}]
    traced = tracing.trace(key="order_id")(lambda rows: rows)
    assert traced(sentinel) is sentinel


def test_replay_refuses_tasks_that_are_not_marked_safe(capture_home):
    from passage import errors, registry, replay

    registry.register_replay("unsafe_dag", [("t1", lambda _: None, "previous")])
    assert replay.preflight("unsafe_dag")
    with pytest.raises(errors.ReplayRefused):
        replay.run("unsafe_dag", "order_id = 1")


def test_hostile_values_survive_the_fast_insert_path(capture_home):
    """Captured values are inlined as SQL literals, so escaping has to be exact."""
    from passage import runtime, snapshot, store

    hostile = "'); DROP TABLE capture.captures; --"
    run_id = f"test__{uuid.uuid4().hex[:8]}"
    rt = runtime.Runtime("d", run_id, "t", "v", "manual", {})
    snap = snapshot.take(
        [{"order_id": 1, "note": hostile, "quote": "it's", "missing": None}], key="order_id"
    )
    store.record(rt, "out", snap, "order_id")

    cells = {c["field_name"]: c["value"] for c in store.captures_for("d", run_id, "1")}
    assert cells["note"] == hostile
    assert cells["quote"] == "it's"
    assert cells["missing"] is None
    # The store is still there, which it would not be if the literal had escaped.
    assert store.list_traces()
