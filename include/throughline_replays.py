"""Replay plans, in one place Throughline can import without importing DAG files.

Each entry is ``(task_id, callable, how it gets its input)``. The task_ids match
the DAG's, so a replay's captures line up column-for-column with a scheduled
run's in the trace grid.
"""

from __future__ import annotations

import throughline
from include.orders_enrichment import steps

throughline.register_replay(
    "orders_enrichment",
    [
        ("extract", steps.extract, "scope"),
        ("normalize", steps.normalize, "previous"),
        ("apply_promo", steps.apply_promo, "previous"),
        ("compute_total", steps.compute_total, "previous"),
    ],
)
