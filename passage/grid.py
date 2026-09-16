"""Assembling one DAG run's captures into the thing you actually look at.

Capture is task-level, because a task boundary is the only place Airflow gives
a clean before and after. The trace is DAG-level. This module is the join
between the two: it takes every task's captures for a single record and lays
them out as fields down the side and tasks across the top.

Two things are rendered from the same captures:

* the **grid** — the record's values in each task's output;
* the **shape strip** — row count, fields added, fields dropped, per task.

The shape strip needs no extra capture and, for understanding an unfamiliar
pipeline, is usually worth more than the values. It is what lets you read
"normalize renames two fields and drops one" off the screen without opening
normalize.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from passage import store


@dataclass
class Shape:
    """What a task did to the record's shape."""

    rows_in: int | None
    rows_out: int
    fields_added: list[str] = field(default_factory=list)
    fields_dropped: list[str] = field(default_factory=list)

    @property
    def fans_out(self) -> bool:
        """One row in, more than one out — the signature of a bad join."""
        return self.rows_in is not None and self.rows_out > self.rows_in


@dataclass
class Column:
    """One task: its output rows for this record, and what it changed."""

    task_id: str
    shape: Shape
    rows: list[dict[str, str | None]] = field(default_factory=list)
    changed: set[str] = field(default_factory=set)

    def value(self, field_name: str, ordinal: int = 0) -> str | None:
        if ordinal < len(self.rows):
            return self.rows[ordinal].get(field_name)
        return None

    def present(self, field_name: str) -> bool:
        return any(field_name in row for row in self.rows)


@dataclass
class Trace:
    """One record's passage through one DAG run."""

    dag_id: str
    run_id: str
    record_key: str
    bundle_version: str
    fields: list[str] = field(default_factory=list)
    columns: list[Column] = field(default_factory=list)

    @property
    def row_counts(self) -> list[int]:
        """The sequence a viewer reads as ``1 -> 1 -> 2 -> 2``."""
        return [c.shape.rows_out for c in self.columns]

    @property
    def breaks(self) -> list[str]:
        """Tasks where the row count changed. Usually the whole answer."""
        return [c.task_id for c in self.columns if c.shape.fans_out]


def _side(cells: list[dict], task_id: str, direction: str) -> list[dict[str, str | None]]:
    """Rebuild a task's rows for one direction, ordered by row_ordinal."""
    rows: dict[int, dict[str, str | None]] = {}
    for cell in cells:
        if cell["task_id"] != task_id or cell["direction"] != direction:
            continue
        rows.setdefault(int(cell["row_ordinal"]), {})[str(cell["field_name"])] = cell["value"]
    return [rows[key] for key in sorted(rows)]


def build(dag_id: str, run_id: str, record_key: str) -> Trace:
    """Assemble the trace for one record in one run."""
    cells = store.captures_for(dag_id, run_id, record_key)
    tasks = [t for t in store.task_order(dag_id, run_id) if any(c["task_id"] == t for c in cells)]

    bundle_version = cells[0]["bundle_version"] if cells else "unknown"

    fields: list[str] = []
    columns: list[Column] = []
    previous: Column | None = None

    for task_id in tasks:
        rows_in = _side(cells, task_id, "in")
        rows_out = _side(cells, task_id, "out")

        in_fields = {f for row in rows_in for f in row}
        out_fields = {f for row in rows_out for f in row}

        # A task's input is normally its upstream's output. When the input was
        # not captured — the first task usually has nothing record-shaped going
        # in — fall back to the previous column, so the shape strip still has
        # something honest to compare against.
        if not rows_in and previous is not None:
            rows_in = previous.rows
            in_fields = {f for row in rows_in for f in row}

        shape = Shape(
            rows_in=len(rows_in) if rows_in else None,
            rows_out=len(rows_out),
            fields_added=sorted(out_fields - in_fields) if in_fields else [],
            fields_dropped=sorted(in_fields - out_fields) if in_fields else [],
        )

        column = Column(task_id=task_id, shape=shape, rows=rows_out)

        # A value counts as changed when the record's first row differs from
        # what the previous task emitted for the same field.
        if previous is not None:
            for name in out_fields:
                if previous.present(name) and column.value(name) != previous.value(name):
                    column.changed.add(name)

        for row in rows_out:
            for name in row:
                if name not in fields:
                    fields.append(name)

        columns.append(column)
        previous = column

    return Trace(
        dag_id=dag_id,
        run_id=run_id,
        record_key=record_key,
        bundle_version=bundle_version,
        fields=fields,
        columns=columns,
    )


def diff(left: Trace, right: Trace) -> dict[str, Any]:
    """Compare the same record through two runs, usually two bundle versions."""
    return {
        "left": left,
        "right": right,
        "row_counts_differ": left.row_counts != right.row_counts,
        "tasks": sorted({c.task_id for c in left.columns} | {c.task_id for c in right.columns}),
    }
