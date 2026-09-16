"""The four steps of orders_enrichment, and the bug hiding in one of them.

The task bodies live here rather than in the DAG file so that they can be run,
and traced, without an Airflow scheduler — see ``tools/local_run.py``. The DAG
file binds them to Airflow and does nothing else.

Adoption, in full, is the ``@passage.trace`` line above each function and the
``{scope}`` placeholder in the two queries that read source tables. Nothing
else about these functions is written for Passage: they take their input, do
their work, write their output and return a handle, exactly as they would have.
"""

from __future__ import annotations

import passage
from passage import session
from passage.table import Table, qualified


@passage.trace(key="order_id", replay_safe=True)
def extract(scope: str = passage.scope.ALL) -> Table:
    """Pull the six fields the pipeline cares about out of the orders table."""
    predicate = passage.scope.resolve(scope)
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.orders_extracted AS
            SELECT order_id, customer_id, order_ts, sku, qty, unit_price_cents
            FROM wh.orders
            WHERE 1=1 AND {predicate}
            """
        )
    finally:
        con.close()
    return Table("orders_extracted", target)


@passage.trace(key="order_id", replay_safe=True)
def normalize(orders: Table) -> Table:
    """Rename two fields into house style, convert cents to dollars, drop sku."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.orders_normalized AS
            SELECT
                order_id,
                customer_id,
                order_ts                   AS ordered_at,
                qty,
                unit_price_cents / 100.0   AS unit_price
            FROM {qualified(orders)}
            """
        )
    finally:
        con.close()
    return Table("orders_normalized", target)


@passage.trace(key="order_id", replay_safe=True)
def apply_promo(orders: Table) -> Table:
    """Attach each customer's active promotion.

    The join matches on customer_id within the promotion's validity window. A
    customer with two overlapping windows matches both, which used to return
    two rows where one went in — silently, since a fanned-out row has no nulls,
    no type errors and no schema change to give it away.

    The QUALIFY keeps the single most valuable promotion per order, so the join
    can match as many rows as it likes and this task still emits one row per
    order. Whether the overlapping promotions should have existed in the first
    place is a separate question, and a real one: this fixes the pipeline, not
    the data.
    """
    # No scope placeholder here on purpose. This task reads a source table
    # (wh.promotions) but reaches it through a join to an already-scoped input,
    # so the join constrains it. Scoping is only ever needed where a query hits
    # a source table unconstrained.
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.orders_promo AS
            SELECT
                o.*,
                p.promo_code,
                p.discount_pct
            FROM {qualified(orders)} o
            LEFT JOIN wh.promotions p
                   ON p.customer_id = o.customer_id
                  AND o.ordered_at::DATE BETWEEN p.valid_from AND p.valid_to
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY o.order_id
                ORDER BY p.discount_pct DESC, p.promo_id
            ) = 1
            """
        )
    finally:
        con.close()
    return Table("orders_promo", target)


@passage.trace(key="order_id", replay_safe=True)
def compute_total(orders: Table) -> Table:
    """Derive the line total after discount."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.orders_enriched AS
            SELECT
                *,
                SUM(discount_pct) OVER (PARTITION BY order_id) AS total_discount_pct,
                ROUND(
                    qty * unit_price
                    * (1 - SUM(discount_pct) OVER (PARTITION BY order_id) / 100.0),
                    2
                ) AS line_total
            FROM {qualified(orders)}
            """
        )
    finally:
        con.close()
    return Table("orders_enriched", target)
