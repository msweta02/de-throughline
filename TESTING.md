# How to test Throughline, and what to point at

Every scenario below has been run on this machine. Each gives the command, the
result to expect, and the one sentence worth saying out loud about it — because
a demo that shows a feature without saying why it matters is a tour, not an
argument.

Prerequisites, once:

```bash
python3 tools/seed_warehouse.py
chmod -R a+rwX include        # the Astro containers run as uid 50000
astro dev start
```

---

## 1. It works without Airflow at all

```bash
python3 tools/local_run.py --run-id demo
```

**Expect** `100 records captured, 0 with more rows out than in`.

**Why it matters:** a capture layer you can only exercise by standing up a
scheduler is a capture layer nobody exercises. The task bodies are plain
functions.

## 2. The shape strip reads a pipeline you have never seen

Open **Browse → Throughline**, or the **Throughline** tab on the DAG page, and
pick any record.

**Expect** four columns, row counts across the top, `+field` and `−field` pills
underneath.

**Why it matters:** none of that came from documentation and none of it came
from reading the SQL. *extract pulls six fields, normalize renames two and
drops one, apply_promo attaches a discount, compute_total derives the line
total.*

## 3. The incident: one record, two rows

```bash
python3 tools/check_demo.py
```

**Expect** exit 0, with `bundle-v1` reproducing `[1,1,2,2]` and `65.0`, and
current code holding `[1,1,1,1]` and `80.0`.

**Why it matters:** no exception, no null, no schema change. This row passes
every data-quality check that is not specifically looking for a duplicate key.

## 4. A fan-out that is *correct*

Open `tickets_join_every_step` and look at ticket `500004`.

**Expect** `1 → 1 → 2 → 2`, breaking at `with_events`.

**Why it matters:** same signature as the bug, and here it is right — the
ticket was reassigned, so it has two events. The tool shows the fan-out and
names the task. Deciding whether it is a defect is still yours. *This is the
strongest thing to show; it is the difference between a detector and an
instrument.*

## 5. Replay cannot write to production

```bash
python3 tools/prove_isolation.py
```

**Expect** `6/6 production writes blocked`, `wh.orders unchanged: 5000 rows
before, 5000 rows after`.

**Why it matters:** replay runs real task code, and real task code contains
inserts. A sandbox schema only protects you if every task remembers to
parameterise its write target. This does not ask them to remember — production
is attached read-only, so the database refuses.

## 6. Tracing changes nothing about the pipeline's output

```bash
python3 tools/local_run.py --run-id off --no-trace
python3 tools/local_run.py --run-id on
```

**Expect** every warehouse table byte-identical between the two runs: same row
counts, same columns, no tables added, no columns added. Throughline writes
only to `include/throughline.duckdb`.

**Why it matters:** if turning tracing on could change what the pipeline
produces, nobody would ever turn it on.

## 7. Replay by any predicate, not just an id

```bash
python3 tools/local_run.py --replay --scope "order_id = 88231"
python3 tools/local_run.py --replay --scope "customer_id = 5000"
python3 tools/local_run.py --replay --scope "order_id IN (88231, 84435, 87108)"
python3 tools/local_run.py --replay --scope "order_id BETWEEN 83232 AND 83235"
```

**Expect** the first two to resolve to record 88231; the last two to capture
three and four records respectively, each browsable from the replay's record
list.

**Why it matters:** the scope is a SQL predicate, so you select the failure
however you can describe it — by key, by customer, by date range.

## 8. The switches

| What | How | Effect |
| --- | --- | --- |
| One run, off | untick **Trace this run** in the Trigger dialog | that run records nothing |
| One run, on | `{"throughline": {"trace": true}}` in the run conf | captures even a scheduled-style run |
| One DAG | `throughline.trace_policy(dag_id, {"manual","scheduled"})` | that DAG records its nightly runs |
| Fleet | `THROUGHLINE_TRACE_SCHEDULED=1` | every DAG does |
| Everything off | `throughline_enabled=false` **plus a restart** | the decorator is not in the call path |

**Expect** a scheduled run to capture nothing unless asked. **Expect the global
switch to need a restart** — it removes the wrapper at import time, so a
running scheduler holds the old decision.

**Why it matters:** capture costs nothing when nobody asked for it — with the
switch off the decorator hands back the original function, not a wrapper that
returns early.

## 9. It survives four DAGs at once

Trigger `orders_enrichment` and all three `tickets_join_*` together.

**Expect** all four to succeed, 100 records each, no bleed between them.

**Why it matters:** DuckDB takes one writer per file, so the losing DAG used
to die on the lock. It now waits it out.

## 10. A trace never mixes DAGs

Ticket `88231` and order `88231` are deliberately the same number.

```bash
python3 tools/local_run.py --replay --dag-id orders_enrichment \
  --scope "order_id = 88231"
python3 tools/local_run.py --replay --dag-id tickets_join_every_step \
  --scope "ticket_id = 88231"
```

**Expect** two traces sharing nothing but the number — different tasks,
different fields.

**Why it matters:** two systems reusing an id space is ordinary. The seeded
collision is there so the claim is testable rather than asserted.

## 11. A wide DAG is still readable

**Expect** nine tasks to render nine shape cards and ten columns, both scrolling
horizontally, with the field column pinned so row labels survive scrolling.

## 12. The whole battery

```bash
python3 -m pytest tests/ -q
python3 -m ruff check . && python3 -m ruff format --check .
python3 tools/prove_isolation.py
python3 tools/check_demo.py
```

**Expect** 14 passed, clean, 6/6 blocked, exit 0.

---

## If you only have five minutes

In the order that shows the most for the least time:

1. **The shape strip on an unfamiliar DAG** (scenario 2). Comprehension is the
   primary use and the easiest thing to underrate.
2. **The correct fan-out in `tickets_join_every_step`** (scenario 4). It is
   what separates an instrument from a rule engine.
3. **`tools/prove_isolation.py`** (scenario 5). Fifteen seconds, and it is
   what anyone who runs real pipelines will actually worry about.
4. **The switch being a removed wrapper, not an early return** (scenario 8).
   `decorated is step` is a stronger claim than any benchmark.

## What this project does *not* claim

Stated here so nobody has to discover it by testing for it:

- **Nothing beyond local `astro dev` has been exercised** — no remote
  executor, no real deployment.
- **The bundle-version picker in the UI does not replay old code.** It stores
  and displays the label; the replay runs whatever the registry currently
  holds. Only `tools/check_demo.py` checks the tag out. See `VERIFY.md`.
- **The `task_policy` zero-edit path is documented, not built.**
- **The plugin endpoints are not authenticated**, which is an Airflow 3.1
  default this project does not fix.
