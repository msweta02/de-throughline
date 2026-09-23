# Demo script

Judging is video-first and judges may never run the repo. Two acts. Act one
teaches the viewer to read the grid so that act two lands without explanation.

Target: **under three minutes.** Every value below is real output from this
repo, not a mock.

## Setup before recording

```bash
python3 tools/seed_warehouse.py
python3 tools/local_run.py --run-id nightly_2026_09_15 --bundle-version bundle-v1
astro dev start
```

The nightly run captures 100 records with the default cap, which is what an
ordinary traced run looks like. Do not pre-run the replays — act two should
create one on camera.

---

## Act one — comprehension (0:00 – 1:10)

> "You've inherited `orders_enrichment`. Four tasks, no docs. What does it
> actually do?"

1. **Start on the DAG's own page**, `orders_enrichment`, and click the
   **Throughline** tab — last in the row, after *Details*. Worth one sentence
   on camera: this is not a separate tool you go somewhere else to use, it is
   a tab on the DAG you were already looking at.
2. **Open the trace for one ordinary order** from last night's run. No replay,
   no setup — the run already captured it.
3. **Read the grid.** Fields down the side, tasks across the top, the record's
   values in the cells.
4. **Read the shape strip out loud.** This is the beat that matters:

   > "extract pulls six fields. normalize renames two and drops one.
   > apply_promo adds a discount. compute_total derives the line total."

   Point out that none of that came from documentation, and none of it came
   from reading the SQL.

5. One line on adoption, on screen: `@throughline.trace(key="order_id")` above
   the task. That is all that was added to this DAG.

---

## Act two — the incident (1:10 – 2:40)

> "Now an order that came out wrong."

6. **Paste the scope**, `order_id = 88231`, pick bundle `bundle-v1`, hit
   replay. It returns in seconds.

7. **The hero shot.** Same grid the viewer already knows how to read, but the
   row count breaks:

   ```
   1  ->  1  ->  2  ->  2
                 ^ apply_promo
   ```

8. **Click `apply_promo`.** Two promotion windows overlapped, the join fanned
   out, and `compute_total` summed both discounts: 15% + 20% = 35%, giving a
   line total of **65.00**.

   Say the quiet part: no exception, no null, no schema change. This row passes
   every data-quality check that is not specifically looking for a duplicate key.

9. **The winning shot.** Replay the same record against `bundle-v2`:

   ```
   1  ->  1  ->  1  ->  1        line_total 80.00
   ```

   The logic was fixed last week. Then say the ambiguous part out loud, because
   it is the realistic part:

   > "The pipeline was wrong. The data was arguably wrong too — three customers
   > still have overlapping promotion windows, and whether they should is a
   > different team's question. The trace is what lets you have that argument."

---

## The 15 seconds that wins over anyone who runs real pipelines (2:40 – 2:55)

10. **Try to write to production during a replay.**

   ```bash
   python3 tools/prove_isolation.py
   ```

   ```
   blocked  INSERT into a production table
            Cannot execute statement of type "INSERT" on database "wh"
            which is attached in read-only mode!
   ...
   6/6 production writes blocked
   wh.orders unchanged: 5000 rows before, 5000 rows after
   ```

   > "Replay runs real task code, and real task code contains inserts. A sandbox
   > schema only protects you if every task remembers to parameterise its write
   > target. This doesn't ask them to remember. Production is attached
   > read-only, so the database refuses."

   If there is time, add: tasks also have to opt in, and a DAG with unmarked
   tasks refuses to replay and names them.

---

## Optional closer, only if the time is there

A full traced run over all 5,000 orders takes about eight seconds and surfaces
all three broken records without being told which to look for:

```
5000 records captured, 3 with more rows out than in
```

Or, if the question in the room is *does this only work on your toy DAG*:
open `tickets_join_every_step` — a support desk, not orders, keyed on
`ticket_id`, written as if by another team. Ticket 500004 reads
`1 -> 1 -> 2 -> 2` because it was reassigned and has two events: the same
signature as the bug, and here it is correct. Say that out loud — the tool
shows the fan-out and names the task; deciding whether it is a defect is
still yours.

If there is time for one more sentence: ticket 88231 carries the same number
as the hero order, on purpose. Two systems reusing an id space is ordinary,
and the two traces share nothing but the number.

## Do not show

- The `task_policy` zero-edit path — it is documented, not built. Claiming it on
  camera would be a lie.
- Anything beyond local `astro dev` — no remote executor or real deployment
  has been exercised. See `VERIFY.md`.
