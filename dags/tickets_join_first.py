"""tickets_join_first — the join is the very first step.

Three tables enter together: the extract reads tickets already joined to the
agent who handled them and the queue they landed in. From the record's point
of view it arrives wide, and every later task only reshapes what is there.

Unrelated to orders_enrichment in every way that matters — different tables,
different key (``ticket_id``), different team's pipeline. That is the point:
the decorator is the same one line.

The DAG file is a binding and nothing else. The task bodies are in
``include/support_desk/steps.py`` so they can be run and traced without a
scheduler.
"""

from __future__ import annotations

from airflow.sdk import Param, dag, task

from include.support_desk import steps

DOC = __doc__


@dag(
    dag_id="tickets_join_first",
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
        # Rendered as a checkbox in Airflow's own Trigger dialog. Params only
        # reach dag_run.conf when a run is triggered with them, so leaving this
        # alone keeps scheduled runs silent exactly as before.
        "throughline_trace": Param(
            True,
            type="boolean",
            title="Trace this run with Throughline",
            description="Capture this run's records. Untick to run the DAG without tracing.",
        ),
    },
)
def tickets_join_first() -> None:
    extract = task(task_id="extract_joined")(steps.jf_extract)
    normalize = task(task_id="normalize")(steps.jf_normalize)
    score_sla = task(task_id="score_sla")(steps.jf_score_sla)

    score_sla(normalize(extract(scope="{{ params.throughline_scope }}")))


tickets_join_first()
