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
    parser.add_argument("--replay", action="store_true", help="read prod read-only, write to scratch")
    parser.add_argument("--sample", default=None, help="records to capture, or 'all'")
    parser.add_argument("--no-trace", action="store_true")
    args = parser.parse_args()

    # Switch 1 is read when the decorator is applied, so it has to be set before
    # the steps module is imported. This is the whole point of that switch: with
    # it off, there is no wrapper to turn off later.
    os.environ["PASSAGE_ENABLED"] = "0" if args.no_trace else "1"

    from passage import runtime  # noqa: E402
    from include.orders_enrichment import steps  # noqa: E402

    run_id = args.run_id or f"local__{uuid.uuid4().hex[:8]}"
    replay_id = f"replay_{uuid.uuid4().hex[:8]}" if args.replay else None

    passage_conf: dict = {"trace": True}
    if args.replay:
        passage_conf |= {"replay": True, "replay_id": replay_id}
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

    print(f"run_id={run_id}  bundle_version={args.bundle_version}"
          f"{'  REPLAY (prod read-only, writes to scratch)' if args.replay else ''}")

    scope = args.scope or "true"
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

    print(f"\ncaptured to {__import__('passage').paths.capture_db()}"
          if not args.no_trace else "\ntracing off: nothing captured")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
