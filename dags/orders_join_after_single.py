"""orders_join_after_single — one source table, then a multi-table join.

The extract reads orders alone, exactly as ``orders_enrichment`` does, and the
second task joins customers and products together in one step. This is the
common shape in practice: a narrow, cheap, scoped extract followed by a
reference join.

The DAG file is a binding and nothing else.
"""

from __future__ import annotations

from airflow.sdk import Param, dag, task

from include.orders_joins import steps

DOC = __doc__


@dag(
    dag_id="orders_join_after_single",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    doc_md=DOC,
    tags=["throughline", "join"],
    params={
        "throughline_scope": Param(
            "true",
            type="string",
            title="Throughline scope",
            description="SQL predicate narrowing the source query. 'true' means every row.",
        ),
    },
)
def orders_join_after_single() -> None:
    extract = task(task_id="extract")(steps.ja_extract)
    join_reference = task(task_id="join_reference")(steps.ja_join_reference)
    compute_total = task(task_id="compute_total")(steps.ja_compute_total)

    compute_total(join_reference(extract(scope="{{ params.throughline_scope }}")))


orders_join_after_single()
