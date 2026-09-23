"""tickets_join_after_single — one source table, then a multi-table join.

The extract reads tickets alone, and the second task joins agents and queues
together in one step. This is the common shape in practice: a narrow, cheap,
scoped extract followed by a reference join.

The DAG file is a binding and nothing else.
"""

from __future__ import annotations

from airflow.sdk import Param, dag, task

from include.support_desk import steps

DOC = __doc__


@dag(
    dag_id="tickets_join_after_single",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    doc_md=DOC,
    tags=["throughline", "support-desk", "join"],
    params={
        "throughline_scope": Param(
            "true",
            type="string",
            title="Throughline scope",
            description="SQL predicate narrowing the source query. 'true' means every row.",
        ),
    },
)
def tickets_join_after_single() -> None:
    extract = task(task_id="extract")(steps.as_extract)
    join_reference = task(task_id="join_reference")(steps.as_join_reference)
    score_sla = task(task_id="score_sla")(steps.as_score_sla)

    score_sla(join_reference(extract(scope="{{ params.throughline_scope }}")))


tickets_join_after_single()
