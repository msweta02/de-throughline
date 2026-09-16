#!/usr/bin/env python3
"""Assert that the demo still demonstrates what the README says it does.

This is the "saved replays become regression tests" idea, run against the repo
itself. Every number checked here is quoted somewhere in README.md or
docs/demo-script.md, so if one of them drifts, the documentation has become
wrong and CI says so rather than a judge finding out on camera.

    python3 tools/check_demo.py
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Switch 1 has to be on before the steps module is imported, or the decorator
# compiles itself out and there is nothing to check.
os.environ.setdefault("PASSAGE_ENABLED", "1")

STEPS = "include/orders_enrichment/steps.py"
HERO = "88231"

failures: list[str] = []


def check(label: str, actual: object, expected: object) -> None:
    ok = actual == expected
    print(f"  {'ok  ' if ok else 'FAIL'}  {label}: {actual!r}")
    if not ok:
        failures.append(f"{label}: expected {expected!r}, got {actual!r}")


def replay_record(bundle_version: str) -> tuple[list[int], dict]:
    """Replay the hero record and return its row counts and final values."""
    from passage import grid, replay

    result = replay.run("orders_enrichment", f"order_id = {HERO}", bundle_version)
    if not result.ok:
        failures.append(f"replay against {bundle_version} did not succeed: {result.note}")
        return [], {}
    trace = grid.build("orders_enrichment", result.replay_id, HERO)
    final = trace.columns[-1].rows[0] if trace.columns and trace.columns[-1].rows else {}
    return trace.row_counts, final


def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)


def main() -> int:
    from passage import session

    subprocess.run(
        [sys.executable, "tools/seed_warehouse.py"], cwd=ROOT, check=True, stdout=subprocess.DEVNULL
    )
    import include.passage_replays  # noqa: F401

    print("\nthe data condition the demo depends on")
    con = session.connect_observer()
    try:
        overlapping = [
            row[0]
            for row in con.execute(
                "SELECT customer_id FROM wh.promotions GROUP BY customer_id "
                "HAVING count(*) > 1 ORDER BY customer_id"
            ).fetchall()
        ]
        hero_customer = con.execute(
            f"SELECT customer_id FROM wh.orders WHERE order_id = {HERO}"
        ).fetchone()
        check("customers with overlapping promo windows", overlapping, [1204, 3877, 5000])
        check(
            "order 88231 is one of them",
            bool(hero_customer) and hero_customer[0] in overlapping,
            True,
        )
        check("orders seeded", con.execute("SELECT count(*) FROM wh.orders").fetchone()[0], 5000)
    finally:
        con.close()

    print("\ncurrent code: the fix holds")
    counts, final = replay_record("current")
    check("row counts", counts, [1, 1, 1, 1])
    check("promo kept", final.get("promo_code"), "LOYALTY-20")
    check("total discount", final.get("total_discount_pct"), "20")
    check("line total", final.get("line_total"), "80.0")

    # Only meaningful in a checkout that has the tags; a shallow clone will not.
    if git("rev-parse", "--verify", "bundle-v1").returncode == 0:
        print("\nbundle-v1: the bug still reproduces")
        git("checkout", "bundle-v1", "--", STEPS)
        try:
            import importlib

            import include.orders_enrichment.steps as steps_mod

            importlib.reload(steps_mod)
            importlib.reload(sys.modules["include.passage_replays"])
            counts, final = replay_record("bundle-v1")
            check("row counts", counts, [1, 1, 2, 2])
            check("total discount", final.get("total_discount_pct"), "35")
            check("line total", final.get("line_total"), "65.0")
        finally:
            git("checkout", "HEAD", "--", STEPS)
    else:
        print("\nbundle-v1 tag not present, skipping the bug-reproduces check")

    if failures:
        print(f"\n{len(failures)} check(s) failed:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nthe demo still demonstrates what it claims")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
