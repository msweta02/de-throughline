"""tickets_join_every_step — a join at every step, widening one table at a time.

agents, then queues, then ticket events. The events join fans out for the
tickets that were reassigned, so those records read ``1 -> 1 -> 2 -> 2`` and
everyone else reads ``1 -> 1 -> 1 -> 1`` — the same signature as the
promotions bug in ``orders_enrichment``, but here it is correct behaviour
rather than a defect. Telling those two apart is the judgement the grid exists
to support.

The DAG file is a binding and nothing else.
"""

from __future__ import annotations

from airflow.sdk import Param, dag, task

from include.support_desk import steps

DOC = __doc__


@dag(
    dag_id="tickets_join_every_step",
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
def tickets_join_every_step() -> None:
    with_agent = task(task_id="with_agent")(steps.es_with_agent)
    with_queue = task(task_id="with_queue")(steps.es_with_queue)
    with_events = task(task_id="with_events")(steps.es_with_events)
    score_sla = task(task_id="score_sla")(steps.es_score_sla)

    joined = with_agent(scope="{{ params.throughline_scope }}")
    score_sla(with_events(with_queue(joined)))


tickets_join_every_step()
