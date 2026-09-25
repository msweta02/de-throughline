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
                               −unit_price_cents
```

Reading four SQL files tells you what a pipeline is *supposed* to do. Watching
one real record move through it tells you what it *does*.

Adoption is one line per task, and nothing else about the DAG changes:

```python
@task
@throughline.trace(key="order_id")
def normalize(orders): ...
```

It lives inside Airflow — a **Throughline** tab on the DAG's own page, beside
*Overview* and *Runs* — so you do not leave the DAG you were looking at.

Built for the Astronomer *Beyond the Dag* hackathon, Plugin Powerhouse
category. Apache 2.0.

### Where to look

| | |
| --- | --- |
| [Quickstart](#quickstart) | running in about a minute, no Airflow needed |
| [How it works](#how-it-works) | the four pieces, and where the data goes |
| [What it costs](#what-it-costs) | measured, not estimated |
| [What was hard](#what-was-hard) | the four things that actually cost time |
| [Verification and limitations](#verification-and-limitations) | what has been executed, and what has not |
| [VERIFY.md](VERIFY.md) | the claim-by-claim record, and the known issues |
| [TESTING.md](TESTING.md) | twelve scenarios with commands and expected results |

---

## Two uses, one mechanism

**Understand a pipeline.** You inherited a DAG nobody documented. Reading four
SQL files tells you what is supposed to happen. Watching one real order move
through the graph tells you what does.

**Explain an incident.** One record came out wrong. Replay it in isolation
against the bundle version that ran that night, see which task broke it, and
then replay it against today's code to find out whether the bug is still there.

Comprehension is the primary use. Debugging is the urgent one.

## What you actually look at

Fields down the side, tasks across the top, the record's values in the cells,
and the shape strip above — the one from the top of this page — showing what
each task did to the record's *shape*:

```
                extract        normalize      apply_promo    compute_total
  rows           1              1 → 1          1 → 2          2 → 2
  added                        +ordered_at    +promo_code    +line_total
                               +unit_price    +discount_pct  +total_discount_pct
  dropped                      −order_ts
                               −sku
                               −unit_price_cents
```

That strip is the documentation nobody wrote: *extract pulls six fields,
normalize renames two and drops one, apply_promo attaches a discount,
compute_total derives the line total.* It needs no extra capture — it is derived
from the same rows as the values — and for an unfamiliar pipeline it is usually
worth more than the values.

And when a record goes wrong, the same strip reads as a diagnosis. Row count
`1 → 1 → 2 → 2` says a join fanned out, and says which task did it, before you
have looked at a single value.

A traced run captures the first 100 records by default, so the record list is
filterable by key — and it names the actual key column, `order_id` here rather
than a generic "record", so you know what you are typing into it. The column
name is read from the captures themselves, not configured.

## Quickstart

Two paths. **A** takes about a minute and needs no Airflow. **B** runs the
plugin inside a real Airflow 3.1 scheduler.

> **Windows:** use `python` wherever these commands say `python3` — Windows
> ships `python`, and `python3` is usually absent or a Store stub. Skip the
> `chmod` line; it is a macOS/Linux/WSL step. Path A is confirmed working on
> Windows; Path B there is untested.

### Path A — the core system, no Airflow (~1 minute)

```bash
pip install -r requirements-dev.txt
python3 tools/seed_warehouse.py      # 5,000-order demo warehouse
python3 tools/local_run.py --run-id nightly
python3 tools/prove_isolation.py
python3 -m pytest tests/ -q
python3 -m ruff check .
```

Expect: the warehouse built, **100 records captured**, **6/6 production writes
blocked**, **14 tests passing**, ruff clean.

`tools/local_run.py` runs the real task bodies with tracing on, without a
scheduler — because a capture layer you can only exercise by standing up
Airflow is a capture layer you will not exercise often enough. It prints the
row-count sequence per record; the rendered grid needs Path B.

**Reproduce the incident:**

```bash
python3 tools/check_demo.py
```

It checks `include/orders_enrichment/steps.py` out of the `bundle-v1` tag,
replays record 88231 against it, restores the current code and replays again:

| | Row counts | Line total |
| --- | --- | --- |
| `bundle-v1` | `1 → 1 → 2 → 2` | **65.00** |
| current | `1 → 1 → 1 → 1` | **80.00** |

This needs the git tags, so it is skipped on a shallow clone.

### Path B — inside Airflow (Astro CLI and Docker)

```bash
python3 tools/seed_warehouse.py
chmod -R a+rwX include        # required: the containers run as uid 50000
astro dev start
```

Then, in the Airflow UI:

1. Open `orders_enrichment` and **toggle it on** — DAGs are paused when first
   created, and a trigger on a paused DAG sits queued forever.
2. **Trigger** it. The Trigger dialog has a *Trace this run with Throughline*
   checkbox, ticked by default.
3. Open the DAG's **Throughline** tab, last in the row after *Details*. It
   lists that DAG's traced runs and nothing else.
4. Click a run, then a record, for the field-by-task grid and the shape strip.
5. For a **replay**, go to **Browse → Throughline** — the replay form lives on
   the index, not the DAG tab. Enter `order_id = 88231` as the scope and
   submit. It returns in seconds, with production attached `READ_ONLY` and
   writes going to a throwaway scratch database.

The `throughline_enabled` Variable is set for you by `airflow_settings.yaml`.

### Notes that will save you time

- **`chmod -R a+rwX include` before `astro dev start`.** The containers run as
  uid 50000; files you created are uid 1000, and the first task dies with
  `Permission denied` without it.
- **Changes under `throughline/` need `astro dev restart`.** That directory is
  baked into the image; `dags/` and `include/` are bind-mounted and live. Skip
  the restart and the containers keep running the previous capture code —
  tasks go green and nothing is captured.
- **If Docker restarts, run `astro dev restart` — not just `docker start`.**
  On WSL a Docker Desktop restart can bring the containers back without
  reattaching the bind mounts. Airflow looks healthy, the plugin serves 200s,
  and the DAG list is silently empty.
- **Changing `throughline_enabled` needs a restart too**, because the switch
  removes the decorator at import time rather than checking at run time.
- **Click the UI rather than curling it.** The plugin renders inside a
  sandboxed iframe, and a link the browser refuses to follow returns a
  perfectly healthy 200.

## Adoption: this has to work on DAGs you did not write

A tool that requires rewriting your pipelines is worthless. Two tiers, both
additive, both optional.

### Tier 1 — tracing. One line per task.

```python
@task
@throughline.trace(key="order_id")
def normalize(orders): ...
```

Nothing else changes. The task runs normally, with the same arguments, the same
return value and the same side effects; Throughline reads what went in and what
came out. This alone gives you the grid and the shape strip.

> **Decorator order matters.** `@throughline.trace` goes *below* `@task`.
> Airflow's `@task` has to be outermost, because it turns the function into
> something that builds a task at parse time — wrapping *that* would run the
> capture wrapper while the DAG file is being parsed rather than inside the
> worker. Getting it backwards is silent enough to be worth a loud error, so
> Throughline raises `DecoratorOrderError` if it sees it.

### Tier 2 — replay scoping. One line per source query.

```sql
select * from orders where 1=1 and {{ params.throughline_scope }}
```

Renders to `true` normally and to `order_id = 88231` during a scoped replay.
Only needed on tasks that read source tables, and only if you want replay to
touch one record instead of the whole table.

The scope is a **SQL predicate, not an id**, so a replay can select whatever
the source table can express:

| Scope | Replays |
| --- | --- |
| `order_id = 88231` | one record, by key |
| `customer_id = 5000` | by a column that is not the record key |
| `order_id IN (88231, 84435, 87108)` | three records in one replay |
| `order_id BETWEEN 83232 AND 83235` | a range |
| `priority = 'P1' AND queue_id = 4` | any combination the table supports |

All of those are captured and every record is browsable from the replay's own
record list — the replays table stores one `record_key` per replay, so the
deep link goes to one of them and the rest are a click away on the run page.

Malformed SQL fails loudly with a parse error pointing at the predicate, and
nothing is written to production either way, because the warehouse is attached
`READ_ONLY` for the whole replay.

There is deliberately **no automatic version of this**. Injecting a predicate
into arbitrary SQL means parsing and rewriting it, which is a research project,
not a feature. One line you can read beats a rewriter you cannot.

### The zero-edit path, not built

Airflow's `task_policy` cluster policy can wrap every task at parse time, which
would enable tracing fleet-wide with no DAG edits at all. Throughline is
structured so that this is a small addition — the decorator is a plain function
wrapper with no DAG-level state. It is claimed here as a design property, not a
feature: it is not implemented and not tested.

## Switching it off

Capture costs nothing when nobody asked for it. Three independent switches, all
defaulting to **off**:

| Switch | Where | Default |
| --- | --- | --- |
| Global | `throughline_enabled` Airflow Variable, read at parse time | off |
| Per task | whether the decorator is applied at all | — |
| Per run | a checkbox in Airflow's Trigger dialog, or `dag_run.conf` | scheduled runs off, manual runs and replays on |

The per-run switch is the one to reach for, because it needs no restart. A DAG
that declares the `throughline_trace` Param gets a **Trace this run with
Throughline** checkbox in Airflow's own Trigger dialog; untick it and that run
executes normally and records nothing. The programmatic form is
`{"throughline": {"trace": true}}` in the run conf, which outranks the
checkbox, and a replay always captures whatever the box says.

Which leaves, for any given run:

| Run type | Produced by | Captures? |
| --- | --- | --- |
| `manual` | ▶ Trigger, the CLI, the API | **Yes** |
| `backfill` | `airflow backfill` | **Yes** — you asked for the rerun |
| `scheduled` | the cron schedule | No |
| `asset_triggered` | an upstream asset updating | No |
| — | a replay | Always |

Airflow 3.1 has exactly those four run types; "triggered" is `manual`, not a
separate kind. The two that do not capture are the two nobody asked for.
Backfill being in the *yes* column is worth knowing before backfilling a wide
date range: untick the box, or pass `{"throughline": {"trace": false}}`.

### Tracing automatic runs

Per DAG, which is usually what you want — one pipeline records its nightly
runs, another does not:

```python
throughline.trace_policy("orders_enrichment", {"manual", "scheduled"})
```

One line, next to `register_replay`. A DAG that says nothing keeps the
defaults, so adding this changes no existing DAG. Note the declared set is
**exhaustive**: the example above stops backfills of that DAG capturing,
because it lists the run types that capture and `backfill` is not among them.

Fleet-wide, if every DAG should trace its scheduled runs:

Scheduled runs stay silent because a scheduled production run is the worst
place for an unasked-for side effect, not because tracing them is wrong. A
deployment that does want them traced opts in:

```bash
THROUGHLINE_TRACE_SCHEDULED=1
```

An environment variable rather than an Airflow Variable, because this is read
*inside every task* and a metadata-database round trip per task would be a
real cost. Like the global switch, the containers have to restart before they
see it.

Both are defaults, and an explicit answer outranks either: an unticked
checkbox or `{"throughline": {"trace": false}}` turns a run off whatever the
policy says.

The global switch matters most, and it is not an early `return` inside a
wrapper. When it is off, the decorator hands back the **undecorated function**:
there is no wrapper in the call path and nothing to cost anything at run time.
The test for this asserts object identity, not behaviour.

```python
decorated = throughline.trace(key="order_id")(step)
assert decorated is step  # passes when the global switch is off
```

Reading an Airflow Variable at parse time is ordinarily an anti-pattern — it is
a database round trip per parse. It is the deliberate trade here, because the
alternative is that the wrapper is always present, which is the exact cost the
switch exists to remove. `THROUGHLINE_ENABLED` is checked first, so local
runs, CI and tests never touch the metadata database.

> **The global switch is not live. Restart Airflow after changing it.**
> Because it removes the wrapper rather than short-circuiting inside one, the
> decision is made when the module is imported — and a long-lived scheduler
> holds that module in `sys.modules` for the life of the process. Flipping the
> Variable on a running deployment changes what the next *read* returns and
> nothing else; the already-decorated functions stay as they were. Measured
> both ways: with the Variable off and no restart a run still captured 4,500
> cells, and after a restart the same run captured 0.
>
> On Astro, note that `astro dev restart` re-applies `airflow_settings.yaml`,
> which sets `throughline_enabled` back to `"true"`. To keep it off across a
> restart, change it there too.

On top of that, a traced normal run captures the **first 100 distinct records**,
not the whole table. A scoped replay lifts the cap, because it is one record by
construction.

The cap is a default, not a limit. Raising it is a reasonable triage move: a
full traced run over all 5,000 demo orders takes about ten seconds and finds
the three broken ones on its own.

```
$ python3 tools/local_run.py --run-id nightly --bundle-version bundle-v1 --sample all

bundle-v1  record 84435  1 -> 1 -> 2 -> 2  breaks=apply_promo  line_total=211.68
bundle-v1  record 87108  1 -> 1 -> 2 -> 2  breaks=apply_promo  line_total=156.43
bundle-v1  record 88231  1 -> 1 -> 2 -> 2  breaks=apply_promo  line_total=65.0

5000 records captured, 3 with more rows out than in
```

That works because capture writes are bulk-inserted as escaped literals rather
than bound one parameter at a time — the difference between about a second and
about a minute per 45,000 cells.

## What it costs

Measured on the demo pipeline, four tasks over 5,000 DuckDB orders:

| | Wall clock | Captured |
| --- | --- | --- |
| Tracing off | 0.31–0.35 s | — |
| Traced, default 100-record cap | 0.99–1.24 s | 4,500 cells |
| Traced, every record | 10.6–11.6 s | 225,000 cells |

Ranges rather than single numbers because repeated runs on the same machine
varied by half again — quoting one figure is how the docs drifted to three
different values for the same operation in the first place.

So roughly **+1 second per 4,500 cells**, and about **14 bytes per cell** on
disk — a traced run of the demo DAG adds ~60 KB to the capture store.

Read the first row carefully before the multiplier alarms you. These tasks do
almost nothing: four `CREATE TABLE`s over 5,000 rows in an embedded database.
Capture costs what it costs *per record*, not as a share of your runtime, so on
a pipeline whose tasks take minutes it disappears into the noise, and on this
one it looks like a 4× slowdown. Judge it by the absolute number.

The rest of the bill:

- **Off costs nothing at all.** The decorator hands back the original function
  object, so there is no wrapper in the call path — `decorated is step`.
- **One Airflow Variable read per DAG parse**, measured at **2.4 ms**, and only
  when `THROUGHLINE_ENABLED` is unset. With it set the check is 0.001 ms and
  never touches the database.
- **No extra process or port.** `fastapi_apps` mounts the app inside the
  existing API server, and FastAPI already ships with Airflow.
- **One more DuckDB file.** The capture store takes an exclusive write lock per
  write, which is why writes are short and retried — see `throughline/locking.py`.

## Tracing a normal run changes nothing

When Throughline traces a scheduled run, the DAG writes to its real tables
exactly as it always does. Throughline reads them and writes only to its own
capture database — a separate DuckDB file, not a schema alongside the
pipeline's output. Measured rather than asserted: fingerprinting every
warehouse table after a traced run and an untraced one gives identical row
counts, identical columns and identical contents, with no table and no column
added. Its own connection attaches the warehouse `READ_ONLY`, so it
cannot write there even by accident, and does not contend for the write lock
the task is using.

If enabling tracing can alter pipeline behaviour, nobody will turn it on.

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

## How it works

Four pieces.

**1. `@throughline.trace` — the capture decorator.** Checks the switches,
snapshots rows on the way in and on the way out, writes them to the capture
store. It captures anything it can read as records: a warehouse relation behind
a `throughline.Table` handle, a list of dicts, or anything with
`to_dict("records")` (pandas and polars, without importing either). Snapshot
failures are logged and
swallowed — a tool that breaks the pipeline it is observing has failed at its job.

**2. The capture table.** Long format, one row per field:

```
capture_id, dag_id, run_id, bundle_version, task_id, direction (in|out),
record_key, row_ordinal, field_name, value, captured_at
```

It costs storage and buys two things worth more: the grid is a pivot rather than
a bespoke assembly step, and comparing two bundle versions is
`where bundle_version in (...)` rather than a second code path.

`row_ordinal` is load-bearing. One record key can map to several rows — that is
the entire point of the fan-out case — so anything keyed only by `record_key`
would collapse exactly the evidence the tool exists to show. It counts *within* a
record, so a fan-out reads as one key with ordinals 0 and 1.

Captured data never goes through XCom. XCom is for control flow; it would blow
the size limit and it is the wrong storage semantics.

**3. The plugin.** `fastapi_apps` serves the app, `external_views` renders it in
the Airflow UI. Server-rendered HTML, Jinja, one `<style>` block — no React, no
build step, no bundle. `GET /traces`, `GET /traces/{dag}/{run}/{record}`,
`POST /replays`, plus the HTML pages.

**4. The demo DAG.** Below.

Capture is task-level because a task boundary is the only place Airflow gives a
clean before and after. The *trace* is DAG-level: the view assembles one run's
worth of task captures, keyed by record, into a single grid.

## The demo DAG and its bug

`orders_enrichment`: `extract → normalize → apply_promo → compute_total` over
5,000 orders in DuckDB.

`apply_promo` joins orders to promotions on `customer_id` within a validity date
range. Nearly every customer has exactly one active promotion. **Three** have two
overlapping windows, so the join fans out — one row in, two rows out — and
`compute_total` sums both discounts.

That choice is load-bearing:

- **It is data-dependent.** 3 records in 5,000. A bug that broke every row would
  show up in any aggregate and would not need a record-scoped tool to find.
- **It is silent.** No exception, no null, no schema change. It passes every
  data-quality check that is not specifically looking for a duplicate key.
- **It surfaces as a row-count change**, which reads faster than a value.

Record **88231** is one of the three.

| | bundle-v1 | bundle-v2 |
| --- | --- | --- |
| row counts | `1 → 1 → 2 → 2` | `1 → 1 → 1 → 1` |
| promo | `AUTUMN-15` *and* `LOYALTY-20` | `LOYALTY-20` |
| total discount | 35% | 20% |
| line total | **65.00** | **80.00** |

Both bundle versions are real git tags, so the same record can be replayed
against each and diffed:

```bash
for v in bundle-v1 bundle-v2; do
  git checkout "$v" -- include/orders_enrichment/steps.py
  python3 tools/local_run.py --replay --scope "order_id = 88231" --bundle-version "$v"
done
git checkout HEAD -- include/orders_enrichment/steps.py
```

```
bundle-v1  1 -> 1 -> 2 -> 2  breaks=apply_promo  discount=35%  line_total=65.0
bundle-v2  1 -> 1 -> 1 -> 1  breaks=none        discount=20%  line_total=80.0
```

The fix deduplicates to the highest-value promotion. Note what it does *not*
settle: three customers still have overlapping promotion windows, and whether
they should is a separate question for whoever owns that table. The logic was
wrong; the data was arguably wrong too. Both readings are defensible, and the
trace shows enough to have the argument with.

## Four DAGs, because one proves nothing

`orders_enrichment` is single-source on purpose: the grid has to be legible
before it is interesting. But "does this work on a DAG that joins, that
somebody else wrote, about something else entirely?" is the next question, so
three more DAGs answer it. They are a **support desk** — tickets, agents,
queues, events — keyed on `ticket_id`. Nothing in them touches `wh.orders`.

| DAG | Shape |
| --- | --- |
| `orders_enrichment` | one source table; the demo's seeded bug |
| `tickets_join_first` | the extract itself joins tickets, agents and queues |
| `tickets_join_every_step` | a join at every step, widening one table at a time |
| `tickets_join_after_single` | single-table extract, then one multi-table join |

`tickets_join_every_step` is the interesting one. Its last join attaches
ticket events, and a reassigned ticket has two, so those records read:

```
with_agent     with_queue     with_events    score_sla
  1 -> 1         1 -> 1         1 -> 2        2 -> 2
```

That is the same signature as the promotions bug — and here it is **correct**.
A reassignment is a real row. The tool does not decide which fan-out is a
defect; it shows you the fan-out and which task caused it, which is the part
you cannot get from reading the SQL. Telling the two apart is the judgement
the grid exists to support.

One ticket is seeded with `ticket_id = 88231`, the same number as the hero
*order*. Two systems reusing an id space is ordinary, and it makes "a trace
never mixes DAGs" testable rather than asserted: replay 88231 in both
pipelines and the grids share nothing but the number.

Run any of them without a scheduler:

```bash
python3 tools/local_run.py --replay --dag-id tickets_join_every_step \
  --scope "ticket_id = 500004"
```

```
local  record 500004  1 -> 1 -> 2 -> 2  breaks=with_events
```

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
first column to the second.

## Verification and limitations

**This runs inside a real Airflow scheduler.** The whole demo path — plugin,
traced run, scoped replay, all four pages — was executed against Astro Runtime
3.1-1 on 22 Sept 2026. Doing that found two defects no test could have caught,
because both depended on objects that only exist inside a live task: `run_type`
arrives as an enum whose `str()` is `"DagRunType.MANUAL"` rather than
`"manual"`, and `airflow.sdk.Variable` cannot be read at DAG-parse time at all.
Both were silent — the DAG went green and captured nothing. Both are fixed.

What has *not* been exercised is anything beyond local `astro dev`: no remote
executor, no real deployment, no concurrency.

CI runs the tests, the isolation proof and `tools/check_demo.py` on every push,
across Python 3.11–3.13. That last one asserts the numbers quoted in this
README — including replaying the hero record against the `bundle-v1` tag to
confirm the bug still reproduces — so if the documentation drifts from the
code, the build fails rather than somebody finding out on camera.

### Verified by execution, inside Airflow 3.1

Astro Runtime 3.1-1, local `astro dev start`:

- The plugin loads and mounts; `external_views` puts it in the nav and on the
  DAG page, and every page renders from real captures.
- A plain manual trigger with no conf captures **4,500 cells over 100
  records across four tasks**, both sides of every boundary.
- `{{ params.throughline_scope }}` renders through TaskFlow — a scoped
  trigger extracted **1** row rather than 5,000.
- Replay works through `POST /throughline/replays`, and the replays table
  records `ok`, `refused` and `failed` from real attempts.
- Four DAGs triggered **simultaneously** all succeed, 100 records each, with
  no bleed between them. A ticket and an order deliberately share the id
  `88231`; the two traces share nothing but the number.
- Scheduled runs capture **nothing** — verified against a DAG on a
  one-minute schedule whose trace checkbox defaults to ticked.
- Tracing does not alter the pipeline's own output: 20 warehouse tables
  fingerprinted after a traced and an untraced run are identical in row
  count, columns and contents, with no table and no column added.

### Known limitations

- **The endpoints are not authenticated.** Airflow 3.1 does not authenticate
  `fastapi_apps` routes and this does not add it. Anyone who can reach the
  API server can read captured values and trigger a replay.
- **The UI cannot replay an older bundle.** A replay started from the plugin
  runs whatever code is deployed now, and is labelled `current`. The form
  used to accept a bundle version and ignore it, which was worse than not
  offering the choice, so the field is gone. To compare versions, check the
  tag out and use `tools/local_run.py --replay --bundle-version`, which
  labels the capture to match the code that actually ran — that is how the
  bundle-v1 and bundle-v2 captures behind the diff view are produced.

  Making the UI do it properly is not a small change: `replay.run` executes
  inside the API server, so a checkout there would mutate a bind-mounted file
  the dag-processor is actively parsing, and a crash mid-replay would leave
  the deployment running old code.
- **A broad scope replays everything.** `scope` is a raw SQL predicate with
  no width guard, so `1=1` re-executes every record.
- **DuckDB only**, and one file with one writer. Wide parallel fan-out inside
  a single DAG is the case not covered.
- **`bundle_version` records as `unknown`** under the `dags-folder` bundle.
  It degrades as designed, costing the diff view its labels rather than the
  run.
- **Nothing beyond local `astro dev`** — no remote executor, no deployment.
- **Page rendering has no automated guard.** Every page is checked by hand;
  nothing in CI asserts it.

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

### Invariants worth not breaking

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

## License

Apache 2.0 — see [LICENSE](LICENSE).
