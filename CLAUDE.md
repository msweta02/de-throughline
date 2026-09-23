# Throughline — working notes

An Airflow 3.1 plugin that traces one record through a DAG. Built for the
Astronomer *Beyond the Dag* hackathon (Plugin Powerhouse category). Submissions
are due **24 Sept 2026, 11:59pm ET**. Solo build, evenings.

**The deadline is the hardest constraint here.** A working 70% demo beats a
broken 100%. If a change does not make one of the demo beats in
`docs/demo-script.md` work better, it is out of scope.

## Read first

- `README.md` — what it is and how it works.
- `TESTING.md` — every scenario, the command, the expected result, and the
  sentence worth saying about it. Start here before a demo.
- `docs/data-flow.md` — `orders_enrichment` end to end: which table each task
  writes, where each snapshot is read from, and what replay changes.
- `ROADMAP.md` — what is deliberately not built, including how a dbt
  integration would fit.
- `VERIFY.md` — **what has actually been executed and what has not.** The demo
  path now runs inside a real Airflow 3.1 scheduler; what is still untested is
  everything beyond local `astro dev`. Check it before trusting any
  Airflow-facing claim.

## Shape of the code

| Path | What |
| --- | --- |
| `throughline/tracing.py` | the decorator. Named `tracing`, not `trace`, so `throughline.trace` is unambiguously the function |
| `throughline/runtime.py` | **the only module that touches Airflow.** Every wrong guess about 3.1 lives here and is a one-file fix |
| `throughline/store.py` | the capture table. Long format, `row_ordinal` within a record |
| `throughline/session.py` | connection policy — the read-only attach that makes replay safe |
| `throughline/grid.py` | assembles captures into the grid and shape strip |
| `throughline/views.py` | pages as plain functions, so they render without FastAPI |
| `throughline/api.py` | thin FastAPI binding |
| `include/orders_enrichment/steps.py` | the real task bodies, runnable without Airflow |
| `dags/orders_enrichment.py` | binding only, no logic |
| `include/support_desk/steps.py` | task bodies for the three join DAGs, a non-orders domain |
| `dags/tickets_join_*.py` | the join DAGs — first-step, every-step, after-single |
| `throughline/locking.py` | waiting out DuckDB's one-writer-per-file lock |
| `throughline/registry.py` | replay plans, and per-DAG `trace_policy` |
| `throughline/templates/dag.html` | the per-DAG page framed by Airflow's DAG tab |
| `plugins/throughline_plugin.py` | two external views: the nav entry and the DAG tab |

## Invariants — do not break these

- **The global switch removes the wrapper**, it does not short-circuit inside
  one. `tests/test_throughline.py` asserts object identity. If that test starts
  asserting behaviour instead, the guarantee has been quietly lost. The cost of
  that design is that the switch is **not live**: the decorator is applied at
  import, a long-lived scheduler caches the module, so changing the Variable
  needs an Airflow restart. Measured, not assumed.
- **`row_ordinal` counts within a record key.** Collapsing it hides the fan-out,
  which is the whole demo.
- **Capture never raises into the task.** Snapshot failures log and swallow.
- **`@throughline.trace` goes below `@task`.** The reverse runs at parse time.
- **Runs nobody asked for capture nothing unless asked.** That is `scheduled`
  and `asset_triggered`; the default set is `{manual, backfill}`.
  `throughline.trace_policy(dag_id, run_types)` opts a single DAG in and
  `THROUGHLINE_TRACE_SCHEDULED` opts the fleet in. Both defaults must stay off. Param defaults are not written into a
  scheduled run's conf, which is what keeps the Trigger checkbox from quietly
  switching every nightly run on — verified, and worth re-verifying if the
  params change.
- **Never put captured data in XCom.**
- **Throughline writes only to `include/throughline.duckdb`.** Its observer
  connection attaches the warehouse `READ_ONLY`.
- **No `target="_top"` in `throughline/templates/dag.html`.** Airflow frames
  it with a sandbox that omits `allow-top-navigation`, so such a link
  silently does nothing when clicked while every server-side check passes.

## Local workflow

```bash
python3 tools/seed_warehouse.py                  # rebuild the warehouse
python3 tools/local_run.py --run-id nightly      # traced run, no scheduler
python3 tools/local_run.py --replay --scope "order_id = 88231"
python3 tools/prove_isolation.py                 # the safety demo
python3 -m pytest tests/ -q && python3 -m ruff check .
```

`include/*.duckdb` is gitignored and rebuildable; deleting it is always safe.

**`astro dev` bind-mounts `dags/`, `include/`, `plugins/` and `tests/` only.**
`throughline/` is baked into the image, so a change to the plugin package needs
`astro dev restart` before the containers see it. Editing a DAG or a step body
is live. Getting this wrong looks like a capture that silently stops working.

## Non-goals

Generic SQL rewriting or AST manipulation. The `task_policy` zero-edit path
(document it, do not build it). Warehouses other than DuckDB. React UI. Auth on
the endpoints. Anything OpenLineage — this is record-level and concrete, which
is precisely what lineage tools do not give you. Tests beyond the sanity
checks.

Multi-record replay used to be listed here and is not a non-goal any more: a
scope is a SQL predicate, so `order_id IN (...)` or a `BETWEEN` already
replays several records and every one of them is browsable. Nothing was built
for it; it falls out of the design. What is still missing is a guard — a
predicate like `1=1` replays the whole table.
