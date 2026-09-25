# Throughline

**Follow one record through a DAG, and see what each task did to it.**

An Airflow 3.1 plugin. Pick one record — an order, a support ticket, anything
with a key — and Throughline shows what every task did to it: which values
changed, which fields appeared and vanished, and the moment one row quietly
became two.

```
                extract        normalize      apply_promo    compute_total
  rows           1              1 → 1          1 → 2          2 → 2
  added                        +ordered_at    +promo_code    +line_total
                               +unit_price    +discount_pct  +total_discount_pct
  dropped                      −order_ts
                               −sku
```

Reading four SQL files tells you what a pipeline is *supposed* to do.
Watching one real record move through it tells you what it *does*.

Adoption is one line per task, and nothing else about the DAG changes. It
lives inside Airflow itself — a **Throughline** tab on the DAG's own page,
next to *Overview* and *Runs*.

```python
@task
@throughline.trace(key="order_id")
def normalize(orders): ...
```

Built for the Astronomer *Beyond the Dag* hackathon, Plugin Powerhouse
category. Apache 2.0.

## How it works

![Three real screenshots from a running Airflow instance: triggering a DAG with the Trace this run with Throughline toggle on, the shape strip showing a task's row count jump from 1 to 2, and a diff comparing the same record replayed against two bundle versions.](docs/flow-screenshots.png)

## What runs where

![Architecture diagram: an Airflow worker running a traced task writes to the warehouse and to throughline.duckdb; the Airflow API server only reads throughline.duckdb; during a replay the warehouse is attached read-only and writes are redirected to a throwaway scratch database.](docs/architecture-diagram.svg)

One rule holds the whole design together: **Throughline reads the pipeline's
data, and writes only its own.** Full mechanics, the capture schema, and the
plugin's HTTP surface are in [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Try it in a minute

```bash
pip install -r requirements-dev.txt
python3 tools/seed_warehouse.py      # 5,000-order demo warehouse
python3 tools/local_run.py --run-id nightly
python3 tools/prove_isolation.py
python3 -m pytest tests/ -q
```

Expect: **100 records captured**, **6/6 production writes blocked**,
**14 tests passing**.

To see the incident that motivated this — a silent join fan-out that changes
an order total from 80.00 to 65.00 — run `python3 tools/check_demo.py`, or
read the full walkthrough in [docs/DEMO.md](docs/DEMO.md).

Running it inside real Airflow (Astro CLI + Docker), the two adoption tiers,
the three off-switches, and measured overhead all live in
[docs/GUIDE.md](docs/GUIDE.md).

## Verified, not assumed

The full demo path — plugin, traced run, scoped replay, all four pages — was
executed against a real Airflow 3.1 scheduler (Astro Runtime 3.1-1) on 22 Sept
2026. CI re-runs the tests, the isolation proof, and the demo's own numbers on
every push, across Python 3.11–3.13.

Known limits: the endpoints are unauthenticated, DuckDB only, and a replay
started from the UI always runs today's code. The claim-by-claim record —
what was executed, and what is still an assumption — is
[VERIFY.md](VERIFY.md); step-by-step reproduction commands are in
[TESTING.md](TESTING.md).

## Read more

| Doc | Covers |
| --- | --- |
| [docs/GUIDE.md](docs/GUIDE.md) | Full quickstart (incl. inside Airflow), adoption tiers, off-switches, cost |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Capture path, capture table schema, modules, HTTP surface, replay isolation |
| [docs/DEMO.md](docs/DEMO.md) | The shape strip explained, the seeded bug, and why there are four demo DAGs |
| [docs/DESIGN_NOTES.md](docs/DESIGN_NOTES.md) | What this deliberately isn't, what was hard to get right, invariants |
| [VERIFY.md](VERIFY.md) | Claim-by-claim: what was executed, what wasn't |
| [TESTING.md](TESTING.md) | Twelve scenarios, with commands and expected output |

## License

Apache 2.0 — see [LICENSE](LICENSE).
