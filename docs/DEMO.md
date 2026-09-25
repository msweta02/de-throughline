# The demo, its bug, and why there are four DAGs

## What you actually look at

Fields down the side, tasks across the top, the record's values in the cells,
and the shape strip above, showing what each task did to the record's *shape*:

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

---
[← Back to README](../README.md) · [Guide: quickstart, adoption, switches](GUIDE.md) · [Architecture](ARCHITECTURE.md)
