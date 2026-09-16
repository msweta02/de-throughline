#!/usr/bin/env python3
"""Build the demo warehouse from scratch, and say what is in it.

Safe to re-run: every statement in the seed is CREATE OR REPLACE.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from passage import paths, session  # noqa: E402

SEED = Path(__file__).resolve().parent.parent / "include" / "sql" / "seed.sql"


def main() -> int:
    con = session.connect()
    try:
        con.execute(SEED.read_text())
        orders = con.execute("SELECT count(*) FROM wh.orders").fetchone()[0]
        promos = con.execute("SELECT count(*) FROM wh.promotions").fetchone()[0]
        overlapping = con.execute(
            """
            SELECT customer_id, count(*) AS promos
            FROM wh.promotions GROUP BY customer_id HAVING count(*) > 1
            ORDER BY customer_id
            """
        ).fetchall()
        print(f"warehouse: {paths.warehouse_db()}")
        print(f"  wh.orders      {orders:>6,} rows")
        print(f"  wh.promotions  {promos:>6,} rows")
        print(
            f"  customers with overlapping promo windows: "
            f"{', '.join(str(c) for c, _ in overlapping)}"
        )
        hero = con.execute(
            "SELECT order_id, customer_id FROM wh.orders WHERE customer_id = 5000"
        ).fetchone()
        print(f"  hero record: order_id={hero[0]} (customer {hero[1]})")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
