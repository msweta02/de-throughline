"""Replay plans, in one place Throughline can import without importing DAG files.

Each entry is ``(task_id, callable, how it gets its input)``. The task_ids match
the DAG's, so a replay's captures line up column-for-column with a scheduled
run's in the trace grid.
"""

from __future__ import annotations

import throughline
from include.orders_enrichment import steps
from include.support_desk import steps as desk

throughline.register_replay(
    "orders_enrichment",
    [
        ("extract", steps.extract, "scope"),
        ("normalize", steps.normalize, "previous"),
        ("apply_promo", steps.apply_promo, "previous"),
        ("compute_total", steps.compute_total, "previous"),
    ],
)


# The three support-desk DAGs. A different team's pipeline, a different key,
# and registered the same way — which is the whole claim about adoption.
throughline.register_replay(
    "tickets_join_first",
    [
        ("extract_joined", desk.jf_extract, "scope"),
        ("normalize", desk.jf_normalize, "previous"),
        ("score_sla", desk.jf_score_sla, "previous"),
    ],
)

throughline.register_replay(
    "tickets_join_every_step",
    [
        ("with_agent", desk.es_with_agent, "scope"),
        ("with_queue", desk.es_with_queue, "previous"),
        ("with_events", desk.es_with_events, "previous"),
        ("score_sla", desk.es_score_sla, "previous"),
    ],
)

throughline.register_replay(
    "tickets_join_after_single",
    [
        ("extract", desk.as_extract, "scope"),
        ("join_reference", desk.as_join_reference, "previous"),
        ("score_sla", desk.as_score_sla, "previous"),
    ],
)
