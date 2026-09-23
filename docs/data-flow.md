# Where the data goes: `orders_enrichment`, end to end

One DAG, four tasks, three files on disk. Every table name, byte count and
cell count below is read from a real run, not illustrative.

## The three files

```
include/
├── warehouse.duckdb              the pipeline's own database. "wh".
│                                 Throughline NEVER writes here.
├── throughline.duckdb            the capture store. Throughline ONLY writes here.
│                                 schema "capture": captures, replays
└── scratch/<replay_id>.duckdb    one throwaway database per replay.
                                  Only exists during and after a replay.
```

Keeping captures in their own file, rather than a schema inside the warehouse,
is the strongest available version of *Throughline is a pure observer*: during
a replay the warehouse is attached `READ_ONLY` and the capture store still
takes writes, because it was never part of that database to begin with.

## A normal traced run

```
                        ┌───────────────────────────────────────────┐
   wh.orders  ─────────►│ extract                                   │
   (5,000 rows)         │   CREATE OR REPLACE TABLE                 │
                        │     wh.orders_extracted AS                │
                        │   SELECT ... FROM wh.orders               │
                        │   WHERE 1=1 AND {scope}                   │
                        └────────────────┬──────────────────────────┘
                                         │ returns Table("orders_extracted","wh")
              ┌──────────────────────────┴───────────────────────────┐
              │  XCom  {"name":"orders_extracted","database":"wh",    │
              │         "__throughline_table__":true}   ~70 bytes     │
              └──────────────────────────┬───────────────────────────┘
                                         ▼
                        ┌───────────────────────────────────────────┐
   wh.orders_extracted ►│ normalize   → wh.orders_normalized        │
                        └────────────────┬──────────────────────────┘
                                         ▼
                        ┌───────────────────────────────────────────┐
   wh.orders_normalized►│ apply_promo → wh.orders_promo             │
   + wh.promotions      │   (the join that fans out)                │
                        └────────────────┬──────────────────────────┘
                                         ▼
                        ┌───────────────────────────────────────────┐
   wh.orders_promo ────►│ compute_total → wh.orders_enriched        │
                        └───────────────────────────────────────────┘
```

That is the pipeline, unchanged. Every intermediate is a real table in the
real warehouse, exactly as it would be without any of this.

## What Throughline does at each boundary

The wrapper runs either side of the task body:

```
   ┌──────────────────────── one traced task ─────────────────────────┐
   │                                                                  │
   │  1. snapshot the INPUT      read the upstream table through a    │
   │     (before the body runs)  connection with wh attached READ_ONLY│
   │                                          │                       │
   │  2. run the real task body  ─────────────┼──► writes wh.<output> │
   │     (its own connection,                 │    through its OWN    │
   │      read-write, closed                  │    connection         │
   │      before step 3)                      │                       │
   │                                          │                       │
   │  3. snapshot the OUTPUT     read the table it just returned      │
   │                                          │                       │
   └──────────────────────────────────────────┼───────────────────────┘
                                              ▼
                              include/throughline.duckdb
                              capture.captures — one row per CELL:
                                dag_id, run_id, bundle_version, task_id,
                                direction(in|out), record_key,
                                record_key_field, row_ordinal,
                                field_name, value, captured_at
```

Three DuckDB connections exist per task, and none of them fight:

| Connection | Attaches | Purpose |
| --- | --- | --- |
| the task's own | `wh` read-write | does the actual work, closed in a `finally` |
| the observer | `wh` **READ_ONLY** | reads rows to snapshot |
| the store | `include/throughline.duckdb` | writes captures |

The observer is read-only partly on principle and partly on mechanics: the
task has just been writing through its own connection, and a second read-write
attachment would contend for DuckDB's exclusive write lock for no reason.

## What one real run produced

100 records, default sampling cap, four tasks:

| Boundary | Fields | Cells |
| --- | --- | --- |
| `extract` out | 6 | 600 |
| `normalize` in | 6 | 600 |
| `normalize` out | 5 | 500 |
| `apply_promo` in | 5 | 500 |
| `apply_promo` out | 7 | 700 |
| `compute_total` in | 7 | 700 |
| `compute_total` out | 9 | 900 |
| | | **4,500** |

`extract` has no `in` row on purpose: its only argument is the scope string,
which is not record-shaped, so there is nothing to snapshot. A task whose
input Throughline cannot read is not an error — it records what it can.

The `in` of each task and the `out` of the one before it are the same table
read twice. That is deliberate: the grid shows what a task *received*, and a
task that reads its input differently from how the previous task wrote it is
exactly the kind of thing worth seeing.

## A replay, and what changes

Only two things move, and both are attachments:

```
   NORMAL RUN                          REPLAY
   ──────────                          ──────
   wh  attached READ-WRITE             wh       attached READ_ONLY   ◄── the guarantee
   writes go to  wh.<table>            writes go to  scratch.<table>
   —                                   scratch  attached read-write

   session.write_target() → "wh"       session.write_target() → "scratch"
```

The task SQL does not change. It is written against `{target}.orders_promo`,
so the same statement lands in the warehouse on a normal run and in a
throwaway database on a replay. A task that hardcodes `wh` instead still runs
normally — and still fails loudly under replay, which is the intended outcome.

```
   POST /throughline/replays {"dag_id": ..., "scope": "order_id = 88231"}
        │
        ├─► preflight: every step marked replay_safe?   no → refuse, name them
        │
        ├─► ATTACH wh READ_ONLY, ATTACH scratch read-write
        │
        ├─► run the registered plan, task by task, capturing as usual
        │     extract → scratch.orders_extracted
        │     normalize → scratch.orders_normalized
        │     apply_promo → scratch.orders_promo
        │     compute_total → scratch.orders_enriched
        │
        └─► write a row to capture.replays  (ok | refused | failed)
```

Captures from a replay go to the same store as a scheduled run's, keyed by the
replay id as `run_id`. That is what makes the diff view a `WHERE
bundle_version IN (...)` rather than a second code path.

## The bit that surprises people

**The record never travels between tasks.** What moves through XCom is a
~70-byte handle naming a table; the rows stay in the warehouse the whole time.
Throughline reads them out of the warehouse at each boundary rather than
intercepting anything in flight. That is why captured data never goes near
XCom, and why tracing a task that returns ten million rows costs the same
XCom as one that returns none.
