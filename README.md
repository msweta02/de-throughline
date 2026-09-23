# Throughline

**Follow one record through a DAG, and see what each task did to it.**

An Airflow 3.1 plugin that traces a single record as it moves through a pipeline
— values changed, fields added or dropped, rows multiplied.

Built for the Astronomer *Beyond the Dag* hackathon. Apache 2.0.

The repository directory is `de-throughline`; the plugin, its package, its
`/throughline` URL prefix and its nav entry are all `throughline`.

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
and a shape strip above showing what each task did to the record's *shape*:

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

```bash
pip install duckdb                       # or: astro dev start, which has it
python3 tools/seed_warehouse.py          # build the demo warehouse
python3 tools/local_run.py --run-id nightly --bundle-version bundle-v1
python3 tools/prove_isolation.py         # show replay cannot write to prod
python3 -m pytest tests/ -q
```

`tools/local_run.py` runs the real task bodies with tracing on, without a
scheduler. It exists because a capture layer you can only exercise by standing
up Airflow is a capture layer you will not exercise often enough.

Inside Airflow:

```bash
astro dev start
```

then set the `throughline_enabled` Airflow Variable (`airflow_settings.yaml`
already does) and trigger `orders_enrichment`.

Two ways in, and the first is the one you will actually use:

- **The DAG's own page.** Open the DAG and click the **Throughline** tab, last
  in the row after *Details*. It lists that DAG's traced runs and nothing
  else, because you arrived from a DAG and every other DAG's runs are noise.
- **Browse → Throughline** for the index across every DAG.

Airflow renders the tab in a sandboxed iframe, so links inside it navigate
within the frame and the DAG header and tabs stay visible while you drill from
run to record to grid.

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
| Per run | `dag_run.conf` | scheduled runs off, manual runs and replays on |

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
full traced run over all 5,000 demo orders takes about eight seconds and finds
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

## Tracing a normal run changes nothing

When Throughline traces a scheduled run, the DAG writes to its real tables
exactly as it always does. Throughline reads them and writes only to its own
capture database — a separate DuckDB file, not a schema alongside the
pipeline's output. Its own connection attaches the warehouse `READ_ONLY`, so it
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
across Python 3.11–3.13. That last one asserts the numbers quoted in this README
and in the demo script — including replaying the hero record against the
`bundle-v1` tag to confirm the bug still reproduces — so if the documentation
drifts from the code, the build fails rather than a judge finding out on camera.

Read **[VERIFY.md](VERIFY.md)** for the claim-by-claim record of what was run
and what was not. It also lists the known limitations — most importantly that
**the plugin endpoints are not authenticated**, which is an Airflow 3.1 default
this project does not fix.

## Layout

```
throughline/  the plugin: decorator, capture store, grid, replay, views
dags/         the demo DAG — a thin binding, no logic
include/      task bodies, seed SQL, replay plans, the DuckDB databases
plugins/      the AirflowPlugin registration
tools/        seed, local run, isolation proof, demo regression check
tests/        sanity checks on the switches, row_ordinal and refusal
docs/         the demo script the video follows
.github/      CI: lint, tests, isolation proof, demo check
```

Contributing notes and the invariants worth not breaking are in
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).
