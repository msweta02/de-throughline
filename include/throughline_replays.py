"""Replay plans, in one place Throughline can import without importing DAG files.

Each entry is ``(task_id, callable, how it gets its input)``. The task_ids match
the DAG's, so a replay's captures line up column-for-column with a scheduled
run's in the trace grid.
"""

from __future__ import annotations

import throughline
from include.orders_enrichment import steps
from include.orders_joins import steps as steps_joins

throughline.register_replay(
    "orders_enrichment",
    [
        ("extract", steps.extract, "scope"),
        ("normalize", steps.normalize, "previous"),
        ("apply_promo", steps.apply_promo, "previous"),
        ("compute_total", steps.compute_total, "previous"),
    ],
)

# The three join DAGs. Registering them means the replay button works on a
# joined pipeline too, not just the single-source demo.
throughline.register_replay(
    "orders_join_first",
    [
        ("extract_joined", steps_joins.jf_extract, "scope"),
        ("normalize", steps_joins.jf_normalize, "previous"),
        ("compute_total", steps_joins.jf_compute_total, "previous"),
    ],
)

throughline.register_replay(
    "orders_join_every_step",
    [
        ("with_customer", steps_joins.je_with_customer, "scope"),
        ("with_product", steps_joins.je_with_product, "previous"),
        ("with_shipment", steps_joins.je_with_shipment, "previous"),
        ("compute_total", steps_joins.je_compute_total, "previous"),
    ],
)

throughline.register_replay(
    "orders_join_after_single",
    [
        ("extract", steps_joins.ja_extract, "scope"),
        ("join_reference", steps_joins.ja_join_reference, "previous"),
        ("compute_total", steps_joins.ja_compute_total, "previous"),
    ],
)
