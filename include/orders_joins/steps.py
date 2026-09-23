"""Task bodies for the three join DAGs.

``orders_enrichment`` is deliberately single-source: act one of the demo has to
teach the grid before it teaches anything else, and one table is the smallest
thing that can do that. These three exist for the question that comes next —
*does this work on a DAG that joins?* — and they cover the three shapes a join
pipeline actually takes:

* **first step** — the extract itself is a join across three tables.
* **every step** — a chain that widens the record one join at a time.
* **after a single-table extract** — one source, then a multi-table join.

All three key on ``order_id`` and all three are traced the same way, which is
the point: nothing about ``@throughline.trace`` changes when the SQL does.

Only the tasks that read a source table carry the ``{scope}`` placeholder, so
a scoped replay narrows at the top and every downstream join inherits it.

The shipments join in ``every_step`` fans out for four orders that shipped in
two parcels. That is not a bug — a split shipment is a real row — which makes
it a useful contrast with the promotions fan-out in ``orders_enrichment``:
same shape in the grid, opposite conclusion.
"""

from __future__ import annotations

import throughline
from throughline import session
from throughline.table import Table, qualified

# --------------------------------------------------------------------------
# Shape 1 — the join is the first step.
# --------------------------------------------------------------------------


@throughline.trace(key="order_id", replay_safe=True)
def jf_extract(scope: str = throughline.scope.ALL) -> Table:
    """Read orders already joined to the customer and the product catalogue."""
    predicate = throughline.scope.resolve(scope)
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.jf_extracted AS
            SELECT
                o.order_id,
                o.customer_id,
                o.order_ts,
                o.qty,
                o.unit_price_cents,
                c.customer_name,
                c.region,
                c.tier,
                p.product_name,
                p.category
            FROM wh.orders o
            JOIN wh.customers c ON c.customer_id = o.customer_id
            JOIN wh.products  p ON p.sku         = o.sku
            WHERE 1=1 AND {predicate}
            """
        )
    finally:
        con.close()
    return Table("jf_extracted", target)


@throughline.trace(key="order_id", replay_safe=True)
def jf_normalize(orders: Table) -> Table:
    """House style: dollars rather than cents, and a single ordered_at."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.jf_normalized AS
            SELECT
                order_id,
                customer_id,
                customer_name,
                region,
                tier,
                product_name,
                category,
                order_ts                  AS ordered_at,
                qty,
                unit_price_cents / 100.0  AS unit_price
            FROM {qualified(orders)}
            """
        )
    finally:
        con.close()
    return Table("jf_normalized", target)


@throughline.trace(key="order_id", replay_safe=True)
def jf_compute_total(orders: Table) -> Table:
    """Line total, plus the tier discount the customer is entitled to."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.jf_enriched AS
            SELECT
                *,
                CASE tier
                    WHEN 'enterprise' THEN 15
                    WHEN 'plus'       THEN 5
                    ELSE 0
                END                                                AS tier_discount_pct,
                round(qty * unit_price, 2)                         AS gross_total,
                round(qty * unit_price * (1 - CASE tier
                    WHEN 'enterprise' THEN 0.15
                    WHEN 'plus'       THEN 0.05
                    ELSE 0.0
                END), 2)                                           AS line_total
            FROM {qualified(orders)}
            """
        )
    finally:
        con.close()
    return Table("jf_enriched", target)


# --------------------------------------------------------------------------
# Shape 2 — a join at every step, widening the record one table at a time.
# --------------------------------------------------------------------------


@throughline.trace(key="order_id", replay_safe=True)
def je_with_customer(scope: str = throughline.scope.ALL) -> Table:
    """Orders joined to customers. One row in, one row out."""
    predicate = throughline.scope.resolve(scope)
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.je_customer AS
            SELECT
                o.order_id,
                o.customer_id,
                o.sku,
                o.qty,
                o.unit_price_cents,
                c.customer_name,
                c.region
            FROM wh.orders o
            JOIN wh.customers c ON c.customer_id = o.customer_id
            WHERE 1=1 AND {predicate}
            """
        )
    finally:
        con.close()
    return Table("je_customer", target)


@throughline.trace(key="order_id", replay_safe=True)
def je_with_product(orders: Table) -> Table:
    """Add the product catalogue. Still one row per order — products are unique."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.je_product AS
            SELECT
                o.order_id,
                o.customer_id,
                o.customer_name,
                o.region,
                o.qty,
                o.unit_price_cents / 100.0 AS unit_price,
                p.product_name,
                p.category
            FROM {qualified(orders)} o
            JOIN wh.products p ON p.sku = o.sku
            """
        )
    finally:
        con.close()
    return Table("je_product", target)


@throughline.trace(key="order_id", replay_safe=True)
def je_with_shipment(orders: Table) -> Table:
    """Add shipments — and this one fans out.

    Four orders shipped in two parcels, so they leave this task with two rows
    where one went in. Nothing is malformed: both shipment rows are real. The
    shape strip shows the fan-out, and whether it is a problem depends on what
    the next task does with it — which is exactly the judgement the grid exists
    to support.
    """
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.je_shipment AS
            SELECT
                o.*,
                s.shipment_id,
                s.carrier,
                s.shipped_on
            FROM {qualified(orders)} o
            LEFT JOIN wh.shipments s ON s.order_id = o.order_id
            """
        )
    finally:
        con.close()
    return Table("je_shipment", target)


@throughline.trace(key="order_id", replay_safe=True)
def je_compute_total(orders: Table) -> Table:
    """Line total per row. A fanned-out order gets one per parcel."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.je_enriched AS
            SELECT
                *,
                round(qty * unit_price, 2) AS line_total
            FROM {qualified(orders)}
            """
        )
    finally:
        con.close()
    return Table("je_enriched", target)


# --------------------------------------------------------------------------
# Shape 3 — a single-table extract, then one step joining several tables.
# --------------------------------------------------------------------------


@throughline.trace(key="order_id", replay_safe=True)
def ja_extract(scope: str = throughline.scope.ALL) -> Table:
    """One source table, exactly like orders_enrichment's extract."""
    predicate = throughline.scope.resolve(scope)
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.ja_extracted AS
            SELECT order_id, customer_id, sku, qty, unit_price_cents
            FROM wh.orders
            WHERE 1=1 AND {predicate}
            """
        )
    finally:
        con.close()
    return Table("ja_extracted", target)


@throughline.trace(key="order_id", replay_safe=True)
def ja_join_reference(orders: Table) -> Table:
    """Two reference tables joined in a single step.

    No ``{scope}`` here: this task reads source tables, but it reaches them
    through a join to an already-scoped input, so it inherits the narrowing.
    """
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.ja_joined AS
            SELECT
                o.order_id,
                o.customer_id,
                o.qty,
                o.unit_price_cents / 100.0 AS unit_price,
                c.customer_name,
                c.region,
                c.tier,
                p.product_name,
                p.category,
                p.list_price_cents / 100.0 AS list_price
            FROM {qualified(orders)} o
            JOIN wh.customers c ON c.customer_id = o.customer_id
            JOIN wh.products  p ON p.sku         = o.sku
            """
        )
    finally:
        con.close()
    return Table("ja_joined", target)


@throughline.trace(key="order_id", replay_safe=True)
def ja_compute_total(orders: Table) -> Table:
    """Line total, and how far the sale price drifted from the list price."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.ja_enriched AS
            SELECT
                *,
                round(qty * unit_price, 2)                             AS line_total,
                round((unit_price - list_price) / list_price * 100, 1) AS price_delta_pct
            FROM {qualified(orders)}
            """
        )
    finally:
        con.close()
    return Table("ja_enriched", target)
