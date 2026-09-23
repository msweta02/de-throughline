"""The pages, as plain functions returning HTML.

Kept apart from the FastAPI binding in :mod:`throughline.api` so the views can be
rendered and checked without a web server — and without FastAPI, which only
exists inside the Airflow image.
"""

from __future__ import annotations

from typing import Any

from throughline import grid, render, store


def index(base: str, default_bundle: str = "current") -> str:
    return render.page(
        "index.html",
        base=base,
        traces=store.list_traces(),
        replays=store.list_replays(limit=20),
        default_bundle=default_bundle,
    )


def records(base: str, dag_id: str, run_id: str, q: str | None = None) -> str:
    q = (q or "").strip()
    rows = store.list_records(dag_id, run_id, contains=q or None)
    return render.page(
        "records.html",
        base=base,
        dag_id=dag_id,
        run_id=run_id,
        records=rows,
        fanned=[r for r in rows if (r["max_rows"] or 1) > 1],
        key_field=store.key_field(dag_id, run_id),
        q=q,
        total=store.count_records(dag_id, run_id),
    )


def trace(base: str, dag_id: str, run_id: str, record_key: str) -> str:
    built = grid.build(dag_id, run_id, record_key)
    others = [r for r in store.runs_for_record(dag_id, record_key) if r["run_id"] != run_id]
    return render.page("trace.html", base=base, trace=built, other_runs=others[:4])


def _final_values(built: grid.Trace) -> dict[str, Any]:
    """What the record looked like when the DAG finished with it."""
    if not built.columns:
        return {}
    last = built.columns[-1]
    return last.rows[0] if last.rows else {}


def diff(base: str, dag_id: str, record_key: str, left: str, right: str) -> str:
    left_trace = grid.build(dag_id, left, record_key)
    right_trace = grid.build(dag_id, right, record_key)

    left_values = _final_values(left_trace)
    right_values = _final_values(right_trace)

    names: list[str] = []
    for source in (left_trace.fields, right_trace.fields):
        for name in source:
            if name not in names:
                names.append(name)

    final_rows = [
        {
            "field": name,
            "left": left_values.get(name),
            "right": right_values.get(name),
            "differs": left_values.get(name) != right_values.get(name),
        }
        for name in names
        if name in left_values or name in right_values
    ]

    return render.page(
        "diff.html", base=base, left=left_trace, right=right_trace, final_rows=final_rows
    )


def trace_json(dag_id: str, run_id: str, record_key: str) -> dict[str, Any]:
    built = grid.build(dag_id, run_id, record_key)
    return {
        "dag_id": built.dag_id,
        "run_id": built.run_id,
        "record_key": built.record_key,
        "bundle_version": built.bundle_version,
        "fields": built.fields,
        "row_counts": built.row_counts,
        "breaks": built.breaks,
        "tasks": [
            {
                "task_id": c.task_id,
                "rows_in": c.shape.rows_in,
                "rows_out": c.shape.rows_out,
                "fields_added": c.shape.fields_added,
                "fields_dropped": c.shape.fields_dropped,
                "changed": sorted(c.changed),
                "rows": c.rows,
            }
            for c in built.columns
        ],
    }
