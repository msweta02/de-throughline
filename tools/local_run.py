#!/usr/bin/env python3
"""Run orders_enrichment end to end without Airflow, with tracing on.

The DAG file binds these same four functions to Airflow and adds nothing to
them, so what this produces is what a traced Airflow run produces. It exists
because a capture layer you can only exercise by standing up a scheduler is a
capture layer you will not exercise often enough.

    python3 tools/local_run.py --run-id nightly --bundle-version v1
    python3 tools/local_run.py --replay --scope "order_id = 88231"
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--bundle-version", default="local")
    parser.add_argument("--dag-id", default="orders_enrichment")
    parser.add_argument("--scope", default=None, help="predicate to narrow the source query")
    parser.add_argument(
        "--replay", action="store_true", help="read prod read-only, write to scratch"
    )
    parser.add_argument("--sample", default=None, help="records to capture, or 'all'")
    parser.add_argument("--no-trace", action="store_true")
    args = parser.parse_args()

    # Switch 1 is read when the decorator is applied, so it has to be set before
    # the steps module is imported. This is the whole point of that switch: with
    # it off, there is no wrapper to turn off later.
    os.environ["PASSAGE_ENABLED"] = "0" if args.no_trace else "1"

    from include.orders_enrichment import steps  # noqa: E402
    from passage import errors, replay, runtime  # noqa: E402
    from passage import scope as scope_mod

    if args.replay:
        # Replays go through passage.replay so that the CLI and the plugin take
        # exactly one path: same replay-safety preflight, same scratch database,
        # same row in the replays table. A replay only the CLI knows about would
        # not show up in the UI, which is how this drifted the first time.
        import include.passage_replays  # noqa: F401

        try:
            result = replay.run(args.dag_id, args.scope or scope_mod.ALL, args.bundle_version)
        except errors.ReplayRefused as exc:
            print(f"refused: {exc}")
            return 2

        print(
            f"run_id={result.replay_id}  bundle_version={args.bundle_version}"
            f"  REPLAY (prod read-only, writes to scratch)"
        )
        for task_id in result.tasks:
            print(f"  {task_id}")
        if not result.ok:
            print(f"replay {result.status}: {result.note}")
            return 1
        _summarise(args.dag_id, result.replay_id, args.bundle_version)
        return 0

    run_id = args.run_id or f"local__{uuid.uuid4().hex[:8]}"

    passage_conf: dict = {"trace": True}
    if args.scope:
        passage_conf["scope"] = args.scope
    if args.sample:
        passage_conf["sample"] = args.sample

    base = dict(
        dag_id=args.dag_id,
        run_id=run_id,
        bundle_version=args.bundle_version,
        run_type="manual",
        conf={"passage": passage_conf},
    )

    print(f"run_id={run_id}  bundle_version={args.bundle_version}")

    scope = args.scope or scope_mod.ALL
    pipeline = [
        ("extract", lambda: steps.extract(scope)),
        ("normalize", lambda handle: steps.normalize(handle)),
        ("apply_promo", lambda handle: steps.apply_promo(handle)),
        ("compute_total", lambda handle: steps.compute_total(handle)),
    ]

    handle = None
    for task_id, step in pipeline:
        runtime.set_override(task_id=task_id, **base)
        handle = step() if task_id == "extract" else step(handle)
        print(f"  {task_id:<14} -> {handle}")
    runtime.clear_override()

    if args.no_trace:
        print("\ntracing off: nothing captured")
        return 0

    _summarise(args.dag_id, run_id, args.bundle_version)
    return 0


def _summarise(dag_id: str, run_id: str, bundle_version: str) -> None:
    """Read back what was captured, the same way the trace view does."""
    from passage import grid, paths, store

    total = store.count_records(dag_id, run_id)
    records = store.list_records(dag_id, run_id)
    if not records:
        print(f"\nnothing captured to {paths.capture_db()}")
        return

    print()
    fanned = [r for r in records if (r["max_rows"] or 1) > 1]

    # A scoped replay is one record, so show it in full; a whole traced run is
    # too many to list, so show the ones whose row count moved.
    for row in (records if total == 1 else fanned)[:5]:
        key = str(row["record_key"])
        trace = grid.build(dag_id, run_id, key)
        last = trace.columns[-1].rows[0] if trace.columns and trace.columns[-1].rows else {}
        detail = "  ".join(
            f"{name}={last[name]}" for name in ("total_discount_pct", "line_total") if name in last
        )
        print(
            f"{bundle_version}  record {key}  "
            f"{' -> '.join(map(str, trace.row_counts))}  "
            f"breaks={','.join(trace.breaks) or 'none'}  {detail}"
        )

    if total > 1:
        # `fanned` is drawn from the listing, which is capped; the count is not.
        print(
            f"\n{total} records captured, "
            f"{len(fanned)} with more rows out than in  ->  {paths.capture_db()}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
