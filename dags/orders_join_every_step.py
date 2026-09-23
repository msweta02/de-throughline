"""orders_join_every_step — a join at every step, widening one table at a time.

customers, then products, then shipments. The shipments join fans out for the
four orders that shipped in two parcels, so the shape strip shows
``1 -> 1 -> 2 -> 2`` for those and ``1 -> 1 -> 1 -> 1`` for everyone else —
the same signature as the promotions bug in ``orders_enrichment``, but here it
is correct behaviour rather than a defect. Telling those two apart is the
judgement the grid exists to support.

The DAG file is a binding and nothing else.
"""

from __future__ import annotations

from airflow.sdk import Param, dag, task

from include.orders_joins import steps

DOC = __doc__


@dag(
    dag_id="orders_join_every_step",
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
def orders_join_every_step() -> None:
    with_customer = task(task_id="with_customer")(steps.je_with_customer)
    with_product = task(task_id="with_product")(steps.je_with_product)
    with_shipment = task(task_id="with_shipment")(steps.je_with_shipment)
    compute_total = task(task_id="compute_total")(steps.je_compute_total)

    joined = with_customer(scope="{{ params.throughline_scope }}")
    compute_total(with_shipment(with_product(joined)))


orders_join_every_step()
