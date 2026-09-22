"""The four steps of orders_enrichment, and the bug hiding in one of them.

The task bodies live here rather than in the DAG file so that they can be run,
and traced, without an Airflow scheduler — see ``tools/local_run.py``. The DAG
file binds them to Airflow and does nothing else.

Adoption, in full, is the ``@throughline.trace`` line above each function and the
``{scope}`` placeholder in the two queries that read source tables. Nothing
else about these functions is written for Throughline: they take their input, do
their work, write their output and return a handle, exactly as they would have.
"""

from __future__ import annotations

import throughline
from throughline import session
from throughline.table import Table, qualified


@throughline.trace(key="order_id", replay_safe=True)
def extract(scope: str = throughline.scope.ALL) -> Table:
    """Pull the six fields the pipeline cares about out of the orders table."""
    predicate = throughline.scope.resolve(scope)
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


@throughline.trace(key="order_id", replay_safe=True)
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


@throughline.trace(key="order_id", replay_safe=True)
def apply_promo(orders: Table) -> Table:
    """Attach each customer's active promotion.

    The join matches on customer_id within the promotion's validity window.
    Nearly every customer has exactly one active promotion, so for nearly every
    order this returns exactly the row it was given. For a customer with two
    overlapping windows it returns two, and nothing here notices: no exception,
    no null, no schema change, and the row still passes every data-quality
    check that is not specifically looking for a duplicate key.
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
            """
        )
    finally:
        con.close()
    return Table("orders_promo", target)


@throughline.trace(key="order_id", replay_safe=True)
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
