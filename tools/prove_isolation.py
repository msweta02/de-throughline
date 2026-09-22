#!/usr/bin/env python3
"""Show that a replay cannot write to production, by trying to.

A sandbox schema is not a guarantee — it only holds if every task remembers to
parameterise its write target, and one hardcoded INSERT undoes it silently. So
this asks the database instead of trusting the convention: during a replay,
production is attached READ_ONLY and every kind of write against it raises.

    python3 tools/prove_isolation.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from throughline import runtime, session  # noqa: E402

# Every way a task might plausibly touch a production table.
ATTEMPTS = [
    # Column-correct on purpose: this has to fail because the database is
    # read-only, not because the statement was malformed.
    (
        "INSERT into a production table",
        "INSERT INTO wh.orders_enriched SELECT * FROM wh.orders_enriched LIMIT 1",
    ),
    ("CREATE a new production table", "CREATE TABLE wh.sneaky AS SELECT 1 AS x"),
    (
        "CREATE OR REPLACE a production table",
        "CREATE OR REPLACE TABLE wh.orders_enriched AS SELECT 1 AS x",
    ),
    ("DELETE from a production table", "DELETE FROM wh.orders"),
    ("UPDATE a production table", "UPDATE wh.orders SET qty = 0"),
    ("DROP a production table", "DROP TABLE wh.promotions"),
]


def main() -> int:
    saved = runtime.set_override(
        dag_id="isolation_check",
        run_id="isolation_check",
        task_id="isolation_check",
        conf={"throughline": {"replay": True, "replay_id": "isolation_check"}},
    )
    try:
        con = session.connect()
        print("replay session: wh attached READ_ONLY, scratch attached read-write\n")

        before = con.execute("SELECT count(*) FROM wh.orders").fetchone()[0]

        blocked = 0
        for label, sql in ATTEMPTS:
            try:
                con.execute(sql)
                print(f"  NOT BLOCKED  {label}")
            except Exception as exc:
                blocked += 1
                reason = str(exc).splitlines()[0].strip()
                print(f"  blocked      {label}\n               {reason}")

        con.execute("CREATE OR REPLACE TABLE scratch.proof AS SELECT 1 AS x")
        scratch_ok = con.execute("SELECT count(*) FROM scratch.proof").fetchone()[0] == 1
        after = con.execute("SELECT count(*) FROM wh.orders").fetchone()[0]
        con.close()

        print(f"\n  {blocked}/{len(ATTEMPTS)} production writes blocked")
        print(f"  scratch writes still work: {scratch_ok}")
        print(f"  wh.orders unchanged: {before} rows before, {after} rows after")
        return 0 if blocked == len(ATTEMPTS) and scratch_ok and before == after else 1
    finally:
        runtime.clear_override(saved)


if __name__ == "__main__":
    raise SystemExit(main())
