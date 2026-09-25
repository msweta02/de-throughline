# Design notes: what this is not, what was hard, and what not to break

## What this is not

Not lineage. Lineage tells you `orders_enriched` depends on `promotions`. That
was never the hard part. The hard part is that *this order* came out at 65.00
and the one next to it came out right, and the answer is two rows where there
should be one. Throughline is record-level and concrete on purpose.

Also not: generic SQL rewriting, warehouses other than DuckDB, a React UI, auth
on the endpoints, column-level lineage, or an LLM explaining the trace.

## What was hard

Not the grid. The grid was an afternoon. What cost time was everything that
was *silently* wrong — and the pattern is that none of it was visible from
outside a running Airflow.

**Two defects that made the DAG go green and capture nothing.** Both surfaced
within an hour of the first real scheduler run, and neither was reachable from
the test suite.

`dag_run.run_type` is a `DagRunType` enum inside a live task, and `str()` on
it yields `"DagRunType.MANUAL"`, not `"manual"` — so the run-type check failed
every comparison and refused every run. Import `DagRunType` outside a task and
stringify it and you get `"manual"`, which is why no test could have caught it.

`airflow.sdk.Variable` cannot be read at DAG-parse time at all; it raises
`ImportError` on `SUPERVISOR_COMMS`. A bare `except` turned that into "the
switch is off", so setting the Variable the README told you to set did
nothing whatsoever. The metadata-DB accessor works at parse time; the Task SDK
one does not.

**The switch is not live, and that is a consequence of its own guarantee.**
Throughline promises the global switch *removes the wrapper* rather than
short-circuiting inside one — so the decision is made at import, and a
long-lived scheduler holds the decorated module in `sys.modules`. Flipping the
Variable on a running deployment changes what the next read returns and
nothing else. Measured in both directions: off without a restart still
captured 4,500 cells; after a restart, zero. The honest fix was documentation,
not code.

**A page that passed every server-side check and was broken in the browser.**
The DAG tab's links did nothing when clicked. `curl` returned 200, the HTML
was correct, the plugin API advertised the view. Airflow frames plugin pages
with `sandbox="allow-scripts allow-same-origin allow-forms"` — no
`allow-top-navigation` — so the `target="_top"` links were refused by the
browser with no error anywhere. The lesson generalises: for a UI change, the
only test that counts is a click.

**DuckDB takes one writer per file.** Running four DAGs at once killed one of
them outright on the warehouse lock. The capture store already retried lock
conflicts; the task's own connection did not. Both share that retry now — and
it retries lock conflicts *only*, because a permission error does not improve
after twenty seconds of backoff.

The thread through all of it: keeping a written record that separated *copied
from something that works* from *ran it, here, and watched it* was the single
most useful habit. Every defect above was found by moving a claim from the
first column to the second. The full record of that separation is
[VERIFY.md](../VERIFY.md).

## Layout

```
throughline/  the plugin: decorator, capture store, grid, replay, views
dags/         four demo DAGs — thin bindings, no logic
include/      task bodies, seed SQL, replay plans, the DuckDB databases
plugins/      the AirflowPlugin registration: nav entry and DAG tab
tools/        seed, local run, isolation proof, demo regression check
tests/        sanity checks on the switches, row_ordinal and refusal
.github/      CI: lint, tests, isolation proof, demo check
```

The repository directory is `de-throughline`; the plugin, its package, its
`/throughline` URL prefix and its nav entry are all `throughline`.

## Invariants worth not breaking

- **The global switch removes the wrapper**, it does not short-circuit inside
  one. The test asserts object identity, not behaviour. The cost of that
  design is that the switch is not live: changing it needs a restart.
- **`row_ordinal` counts within a record key.** Collapsing it hides the
  fan-out, which is the whole point.
- **Capture never raises into the task.** Snapshot failures log and swallow.
- **`@throughline.trace` goes below `@task`.** The reverse runs at parse time,
  and raises `DecoratorOrderError` rather than failing quietly.
- **Captured data never goes through XCom**, which carries a ~70-byte handle.
- **Throughline writes only to `include/throughline.duckdb`.**

---
[← Back to README](../README.md) · [Guide: quickstart, adoption, switches](GUIDE.md) · [Architecture](ARCHITECTURE.md) · [Demo & bug walkthrough](DEMO.md)
