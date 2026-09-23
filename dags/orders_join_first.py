"""orders_join_first — the join is the very first step.

Three tables enter the pipeline together: the extract reads orders already
joined to customers and to the product catalogue. From the record's point of
view it arrives wide, and every later task only reshapes what is there.

The DAG file is a binding and nothing else. The task bodies are in
``include/orders_joins/steps.py`` so they can be run and traced without a
scheduler.
"""

from __future__ import annotations

from airflow.sdk import Param, dag, task

from include.orders_joins import steps

DOC = __doc__


@dag(
    dag_id="orders_join_first",
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
def orders_join_first() -> None:
    extract = task(task_id="extract_joined")(steps.jf_extract)
    normalize = task(task_id="normalize")(steps.jf_normalize)
    compute_total = task(task_id="compute_total")(steps.jf_compute_total)

    compute_total(normalize(extract(scope="{{ params.throughline_scope }}")))


orders_join_first()
