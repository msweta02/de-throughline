# Where this could go

Ordered by value per hour of work, not by ambition. Everything here is
deliberately *not* built.

## Not on this list, on purpose

Generic SQL rewriting or AST manipulation. Warehouses other than DuckDB. A
React UI. Auth on the endpoints. Anything OpenLineage — this is record-level
and concrete, which is precisely what lineage tools do not give you.

Those are scope decisions rather than a backlog. A record-level tool that also
tried to be a lineage tool would be worse at both.

## Worth doing next

**Make the bundle-version picker replay old code.** The one real hole. The UI
accepts a bundle version, stores it and shows it on a pill, but
`throughline/replay.py` runs whatever `registry.plan()` currently holds — so
picking `bundle-v1` returns the *fixed* answer under a `bundle-v1` label,
which is worse than not offering it. The mechanism already exists in
`tools/check_demo.py`: check `include/orders_enrichment/steps.py` out of the
tag, `importlib.reload`, replay, restore. Roughly fifteen lines to lift into
`throughline/replay.py`.

**Guard a wide replay scope.** `scope` is a raw SQL predicate with no width
limit, so `1=1` re-executes every record on an endpoint with no
authentication. Count the matches first and refuse above a threshold unless
the caller explicitly opts in.

**Assert that the pages render, in CI.** Four pages are confirmed by hand
before every demo and by nothing else. A broken template reaches the camera
silently. A few lines in `tools/check_demo.py` calling each view and asserting
on the grid text would close it.

**Do the uid-50000 dance for the user.** A fresh clone's first
`astro dev start` dies on `Permission denied` because the containers run as a
different user than the one that created the DuckDB files. Documented, not
automated.

## Worth doing if this outlives the hackathon

**A real store.** One DuckDB file with an exclusive write lock is fine for a
linear DAG and wrong for wide parallel fan-out. Postgres is the obvious swap;
`throughline/store.py` is the only module that would change.

**Authentication.** Airflow 3.1 does not authenticate `fastapi_apps` routes
and this does not add it. Anyone who can reach the API server can read
captured values and trigger a replay.

**Retention.** Nothing ever deletes a capture. A traced run of 100 records
over four tasks is ~60 KB, which is nothing until it is a year of them.

**The `task_policy` zero-edit path.** A cluster policy could wrap every task
at parse time, giving fleet-wide tracing with no DAG edits. The decorator is a
plain function wrapper with no DAG-level state, so this is a small addition —
but it is currently claimed as a design property, not a feature, and should
stay that way until someone runs it.

**Capture on failure.** Today a task that raises captures its input and never
its output. The input alone is often exactly what you want when debugging a
crash, and it is already being written — it just is not surfaced as "this run
died here, with this record in hand".

## Interesting, unproven

**Diffing two records rather than two runs.** The grid answers "what happened
to this record". The next question is usually "why did this one work and that
one not", which is the same view with a different pair of columns.

**Column-level provenance from the shape strip.** Throughline already knows
which task added which field. Chaining that across a DAG gives a per-field
lineage that is derived from observed behaviour rather than parsed SQL — the
opposite trade from every lineage tool, and worth something precisely because
it is concrete.

## dbt

Worth its own heading, because "we use dbt" is the most likely reason someone
cannot adopt this today.

**What breaks.** Tier 1 is a decorator on a Python task function. A dbt model
is SQL executed inside the warehouse by `dbt run`; under Cosmos each model
becomes an Airflow task, but there is no Python function whose arguments and
return value can be read. `@throughline.trace` has nothing to wrap.

**What does not break.** The capture primitive is not Python-shaped. It takes
a *relation* and a key and turns it into rows — see `snapshot.take` with a
`Table` handle. A dbt model materialises exactly that: a named relation, with
a known set of upstream relations. So the thing Throughline needs from a task,
dbt already produces.

The missing piece is the wiring, and it maps unusually cleanly:

| Throughline | dbt equivalent |
| --- | --- |
| task boundary | a model, and the models it selects from |
| the DAG's task graph | `manifest.json`, which dbt writes on every run |
| `{{ params.throughline_scope }}` | `{{ var('throughline_scope', 'true') }}` in the model |
| replay writing to a scratch database | `dbt run --target scratch`, which dbt supports natively |
| `replay_safe=True` | a model tag, e.g. `{{ config(tags=['replay_safe']) }}` |

So a dbt integration would be a post-run hook that reads `manifest.json` for
the model graph, snapshots each model's relation and its parents keyed on a
configured column, and writes them to the same capture store. The grid, the
shape strip, the diff view and the replay isolation story all work unchanged,
because none of them know or care where the rows came from.

**What it would cost.** The snapshot is a `SELECT` per model per run, which is
the same cost tracing already pays. The honest risk is keys: a Python task
hands Throughline a handle and a key argument, whereas a dbt model would need
the key configured per model, and models that aggregate have no record key at
all — those would record shape and not values, which is what the tool already
does for an unkeyed task.

**Not started.** Listed here because the design happens to fit, not because
any of it has been tried.
