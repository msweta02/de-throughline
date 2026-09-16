"""orders_enrichment — the DAG you inherited and nobody documented.

Four tasks: extract, normalize, apply_promo, compute_total. Reading them tells
you what they are supposed to do. Watching one order move through them tells you
what they do — which is the point of the plugin this DAG exists to demonstrate.

The DAG file is a binding and nothing else. The task bodies are in
``include/orders_enrichment/steps.py``, so they can be run and traced without a
scheduler; everything here is Airflow wiring.

Adoption, in full:

* ``@passage.trace`` sits above each step function, *below* ``@task`` — see the
  note in ``passage/tracing.py`` for why that order and not the other one.
* ``params.passage_scope`` renders to ``true`` on a normal run and to a
  predicate like ``order_id = 88231`` during a scoped replay. Only the task
  that reads a source table needs it.

Nothing else about these tasks was written for Passage.
"""

from __future__ import annotations

from airflow.sdk import Param, dag, task

from include.orders_enrichment import steps

DOC = __doc__


@dag(
    dag_id="orders_enrichment",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    doc_md=DOC,
    tags=["passage", "demo"],
    params={
        # Tier 2 adoption, the whole of it. Renders to "true" unless a replay
        # overrides it through the DAG run conf.
        "passage_scope": Param(
            "true",
            type="string",
            title="Passage scope",
            description="SQL predicate narrowing the source query. 'true' means every row.",
        ),
    },
)
def orders_enrichment() -> None:
    # ``task(...)`` applied to the already-traced functions keeps @task as the
    # outermost decorator, which is the order Passage requires.
    extract = task(task_id="extract")(steps.extract)
    normalize = task(task_id="normalize")(steps.normalize)
    apply_promo = task(task_id="apply_promo")(steps.apply_promo)
    compute_total = task(task_id="compute_total")(steps.compute_total)

    # The templated string is rendered by Airflow before the task runs, so the
    # task body receives a predicate, never a template.
    extracted = extract(scope="{{ params.passage_scope }}")
    normalized = normalize(extracted)
    promoted = apply_promo(normalized)
    compute_total(promoted)


orders_enrichment()
