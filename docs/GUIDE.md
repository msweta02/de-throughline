# Guide: running it, adopting it, turning it off

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

---

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

---

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

---

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

---
[← Back to README](../README.md) · [Architecture](ARCHITECTURE.md) · [Demo & bug walkthrough](DEMO.md)
