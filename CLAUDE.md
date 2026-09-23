# Throughline — working notes

An Airflow 3.1 plugin that traces one record through a DAG. Built for the
Astronomer *Beyond the Dag* hackathon (Plugin Powerhouse category). Submissions
are due **24 Sept 2026, 11:59pm ET**. Solo build, evenings.

**The deadline is the hardest constraint here.** A working 70% demo beats a
broken 100%. If a change does not make one of the demo beats in
`docs/demo-script.md` work better, it is out of scope.

## Read first

- `README.md` — what it is and how it works.
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

## Invariants — do not break these

- **The global switch removes the wrapper**, it does not short-circuit inside
  one. `tests/test_throughline.py` asserts object identity. If that test starts
  asserting behaviour instead, the guarantee has been quietly lost.
- **`row_ordinal` counts within a record key.** Collapsing it hides the fan-out,
  which is the whole demo.
- **Capture never raises into the task.** Snapshot failures log and swallow.
- **`@throughline.trace` goes below `@task`.** The reverse runs at parse time.
- **Never put captured data in XCom.**
- **Throughline writes only to `include/throughline.duckdb`.** Its observer
  connection attaches the warehouse `READ_ONLY`.

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
is precisely what lineage tools do not give you. Multi-record replay. Tests
beyond the sanity checks.
