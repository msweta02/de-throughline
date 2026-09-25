# Architecture

Three processes, three files, and one rule: **Throughline reads the pipeline's
data and writes only its own.**

![Architecture diagram: an Airflow worker running a traced task writes to the warehouse and to throughline.duckdb; the Airflow API server only reads throughline.duckdb; during a replay the warehouse is attached read-only and writes are redirected to a throwaway scratch database.](architecture-diagram.svg)

Both boxes are ordinary Airflow processes — there is no Throughline daemon.
The worker writes captures as a side effect of running your task; the API
server only ever reads them.

## The capture path, per task

The decorator wraps the task body and does three things around it:

1. **Snapshot the input** — read the upstream relation through a connection
   with the warehouse attached `READ_ONLY`.
2. **Run the real task body**, untouched, writing wherever it already wrote.
3. **Snapshot the output** — read the relation it just returned.

Three DuckDB connections exist per task and none contend: the task's own
(read-write, closed in a `finally`), the observer (`READ_ONLY`), and the store.
The observer is read-only partly on principle and partly on mechanics — a
second read-write attachment would fight for DuckDB's exclusive write lock for
no reason.

**The record never travels between tasks.** XCom carries a ~70-byte handle
naming a relation; the rows stay in the warehouse. That is why tracing a task
returning ten million rows costs the same XCom as one returning none.

## The capture table

Long format, one row per field:

```
capture_id, dag_id, run_id, bundle_version, task_id, direction (in|out),
record_key, record_key_field, row_ordinal, field_name, value, captured_at
```

It costs storage and buys two things worth more: the grid is a pivot rather
than a bespoke assembly step, and comparing two bundle versions is
`where bundle_version in (...)` rather than a second code path.

`row_ordinal` is load-bearing. One record key can map to several rows — that
is the entire point of the fan-out case — so anything keyed only by
`record_key` would collapse exactly the evidence the tool exists to show. It
counts *within* a record, so a fan-out reads as one key with ordinals 0 and 1.

`record_key_field` is what lets the record list head its column `order_id` or
`ticket_id` rather than a generic "record". It is read from the captures, not
configured anywhere.

Captured data never goes through XCom. XCom is for control flow; captured
values would blow its size limit and it is the wrong storage semantics.

## Modules, and why they are separate

| Module | Responsibility |
| --- | --- |
| `tracing.py` | the decorator: switches, snapshot, store. Never raises into the task |
| `runtime.py` | **the only module meant to touch Airflow.** Every 3.1 guess lives here |
| `snapshot.py` | turn a relation, a list of dicts, or a dataframe into rows |
| `store.py` | the capture table, long format, bulk-inserted |
| `session.py` | connection policy — the `READ_ONLY` attach that makes replay safe |
| `locking.py` | waiting out DuckDB's one-writer-per-file lock |
| `grid.py` | assemble captures into the grid and the shape strip |
| `views.py` | pages as plain functions, so they render without FastAPI |
| `api.py` | the FastAPI binding, and nothing else |
| `registry.py` | replay plans and per-DAG trace policy |

`views.py` returning strings rather than responses is deliberate: every page
can be rendered and asserted on in a test with no web server, which is how the
grid was built before Airflow was ever running.

`config.py` also imports Airflow, which breaks the one-module rule — and that
is exactly how one of the two silent defects landed outside the blast radius
the design intended. It is recorded rather than hidden.

## The plugin surface

`fastapi_apps` mounts the app inside Airflow's existing API server — no second
process, no second port, and FastAPI already ships with Airflow.
`external_views` registers two entries: the **Browse → Throughline** nav item
and the per-DAG tab, the latter rendered by Airflow in a sandboxed iframe.

```
GET  /                                    index: replay form, traces, replays
GET  /dags/{dag_id}                       one DAG's runs (framed by the tab)
GET  /runs/{dag_id}/{run_id}              records in a run, filterable
GET  /runs/{dag_id}/{run_id}/{record}     the grid and shape strip
GET  /diff?dag_id&record_key&left&right   one record across two runs
GET  /replays                             replay history
POST /replays                             start one
GET  /traces, /traces/{dag}/{run}[/{rec}] the same data as JSON
```

Server-rendered HTML, Jinja, one `<style>` block. No React, no build step.

## What replay changes

Exactly two attachments, and nothing else:

```
NORMAL RUN                          REPLAY
wh  attached READ-WRITE             wh       attached READ_ONLY   ◄── the guarantee
writes go to  wh.<table>            writes go to  scratch.<table>
—                                   scratch  attached read-write
```

Task SQL is written against `{target}`, so the same statement lands in the
warehouse on a normal run and in a throwaway database on a replay. A task that
hardcodes `wh` still runs normally — and still fails loudly under replay, which
is the intended outcome.

Capture is task-level because a task boundary is the only place Airflow gives a
clean before and after. The *trace* is DAG-level: the view assembles one run's
worth of task captures, keyed by record, into a single grid.

## Replay cannot write to production

Replay re-executes real task code, and that code contains inserts. A sandbox
schema alone is **not** sufficient: it only works if every task parameterises its
write target, and one hardcoded `insert into analytics.orders_enriched` silently
corrupts production.

So the guarantee is mechanical rather than conventional. During a replay,
production is attached `READ_ONLY` and a per-replay scratch database takes the
writes. The database refuses the write; no task has to remember anything.

```
$ python3 tools/prove_isolation.py
replay session: wh attached READ_ONLY, scratch attached read-write

  blocked      INSERT into a production table
               Cannot execute statement of type "INSERT" on database "wh" which is attached in read-only mode!
  blocked      CREATE a new production table
  blocked      CREATE OR REPLACE a production table
  blocked      DELETE from a production table
  blocked      UPDATE a production table
  blocked      DROP a production table

  6/6 production writes blocked
  scratch writes still work: True
  wh.orders unchanged: 5000 rows before, 5000 rows after
```

**On a real warehouse**, the equivalent is a role, not an attachment: run replay
under a role with read-only grants on production schemas and write access only
to a scratch schema. Same guarantee, enforced by the warehouse either way. The
DuckDB `ATTACH ... (READ_ONLY)` here is the single-file version of that idea.

And on top of the mechanical guarantee, tasks opt in:

```python
@throughline.trace(key="order_id", replay_safe=True)
```

A DAG with untagged tasks can still be **traced**; it refuses to **replay**, and
names the tasks that are not marked:

```
ReplayRefused: these tasks are not marked replay_safe, so Throughline will not
re-execute them: apply_promo
```

Refusing is the right default. The cost of refusing is an error message; the
cost of not refusing is a corrupted production table.

---
[← Back to README](../README.md) · [Guide: quickstart, adoption, switches](GUIDE.md) · [Demo & bug walkthrough](DEMO.md)
