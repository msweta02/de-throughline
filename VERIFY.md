# What has been verified, and what has not

Airflow 3.1 is recent and its plugin and context APIs are easy to hallucinate.
This file records exactly which claims in this repo have been executed and which
are still assumptions, so that a reviewer does not have to guess.

**Headline: this now runs inside a real Airflow scheduler.** On 22 Sept 2026 the
whole demo path was executed against Astro Runtime 3.1-1 (Airflow 3.1.0+astro.1)
in Docker — plugin, traced DAG run, scoped replay and all four pages. Doing so
found two defects that no test could have caught, both now fixed and both
described below. What has *not* been exercised is anything beyond local
`astro dev`: no remote executor, no real deployment, no concurrency.

## Verified by execution, outside Airflow

| Claim | How |
| --- | --- |
| Replay cannot write to production | `tools/prove_isolation.py` — six kinds of write against a `READ_ONLY` attached database, all rejected by DuckDB, with row counts unchanged |
| Scratch writes still work during replay | same script |
| The seeded bug fans out for exactly 3 of 5,000 customers | `tools/seed_warehouse.py`, then a count of orders with more than one output row |
| `1 -> 1 -> 2 -> 2` for record 88231 under bundle-v1 | `tools/local_run.py --replay --scope "order_id = 88231"` |
| `1 -> 1 -> 1 -> 1` for the same record under bundle-v2 | same, after the fix |
| `row_ordinal` keeps both rows of a fanned-out record | `tests/test_throughline.py` |
| The global switch removes the wrapper rather than short-circuiting it | `tests/test_throughline.py` — asserts the returned object *is* the original function |
| Scheduled runs capture nothing by default | `tests/test_throughline.py` |
| Sampling caps by record, not by row | `tests/test_throughline.py` |
| Tracing does not change what a task returns | `tests/test_throughline.py` |
| Replay refuses tasks that are not marked safe, by name | `tests/test_throughline.py` |
| The demo's own numbers are guarded | `tools/check_demo.py`, run by CI — seed condition, the fix holding on current code, and the bug reproducing against the `bundle-v1` tag |
| A full traced run finds all 3 broken records unaided | `tools/local_run.py --sample all` over 5,000 orders, 10.6–11.6s across repeated runs on the development machine |
| Captured values are escaped safely on the bulk-insert path | `tests/test_throughline.py` — a value containing `'); DROP TABLE ...` round-trips intact and the store survives |
| CLI and plugin replays take the same path | `tools/local_run.py --replay` calls `throughline.replay.run`, so both hit the same preflight and both appear in the replays table |

## Verified by execution, inside Airflow 3.1

Astro Runtime 3.1-1, local `astro dev start`, 22–23 Sept 2026.

| Claim | How it was shown |
| --- | --- |
| The plugin loads and mounts | `GET /throughline/` returns 200 and the page titled *Throughline* |
| `external_views` puts it in the nav | `airflow plugins` shows `href: throughline/`, `category: browse`, agreeing with `url_prefix` |
| The DAG parses with the decorator applied | `airflow dags list` shows `orders_enrichment`, no import error |
| `@task` above `@throughline.trace` executes the wrapper in the worker | a plain manual trigger captured on all four tasks |
| An ordinary traced run captures | plain trigger, no conf: 4,500 cells over 100 distinct records across 4 tasks, both `in` and `out` |
| The default sampling cap applies in a real run | the same run stopped at 100 records, not 5,000 |
| TaskFlow renders `{{ params.throughline_scope }}` | a trigger scoped to `order_id = 88231` extracted **1** row, not 5,000, and captured only record 88231 |
| `dag_run.conf` overrides a declared `param` of the same name | same run — the predicate arrived from conf |
| Replay works through the plugin | `POST /throughline/replays` returned `status: ok` for record 88231 across all four tasks |
| The replays table records outcomes | `refused`, `failed` and `ok` rows all written from real attempts |
| All four pages render inside Airflow | index, records, trace and diff each returned 200 from real captures |
| The diff view shows the incident | diff of a bundle-v1 replay against a bundle-v2 replay renders 65.0 against 80.0 |
| A failed replay-plan import costs only the replay button | the plan import failed once at plugin load; the trace UI kept serving and `POST /replays` returned `refused` with a clear reason |
| The endpoints are not authenticated | plain `curl`, no credentials, 200 — see *Known limitations* |
| Tracing works on an unrelated team's DAG | the three `tickets_join_*` DAGs key on `ticket_id`, join tickets/agents/queues/events, and touch no orders table; all captured on every task |
| A join fan-out surfaces the same as the seeded bug | ticket 500004 (reassigned, two events) read `1 -> 1 -> 2 -> 2`, attributed to `with_events` |
| Four DAGs run concurrently | `orders_enrichment` plus all three `tickets_join_*` triggered simultaneously (`par02`); all succeeded, each capturing 100 records |
| A trace never mixes DAGs | `ticket_id = 88231` is seeded to collide with the hero `order_id = 88231`. Replayed in both pipelines, each grid shows only its own tasks and fields, and `runs_for_record` returned no foreign-DAG rows |
| The record list names its real key column | the header reads `order_id`, read from the captures rather than configured |
| Filtering the record list works | `?q=83245` returned *1 of 100 records*; a non-matching filter says so rather than rendering an empty table |
| Throughline appears as a tab on Airflow's own DAG page | `destination: "dag"` puts it after *Details* at `/dags/<dag_id>/plugin/throughline-dag`; confirmed visually in the browser, not only by API |
| The DAG tab shows one DAG's runs | each of the four DAGs' tabs listed only its own runs; an unknown dag_id gets the empty-state panel rather than an error |
| Links inside the DAG tab navigate | clicking a run opens its record list inside the frame, with the DAG header and tabs still visible |
| The global switch genuinely stops capture | with `throughline_enabled=false` **and a restart**, a normal manual run captured 0 cells; with it true and a restart, 4,500 |
| The global switch is not live | changing the Variable without restarting had no effect in either direction — off-without-restart still captured 4,500, on-without-restart still captured 0 |
| The Trigger-dialog checkbox switches capture per run | Airflow reports `throughline_trace` as a boolean param; unticked captured 0 cells, ticked 4,500, and no conf at all still followed the manual-run default of 4,500 — all without a restart |
| Scheduled runs capture nothing, even with the checkbox defaulting to ticked | a DAG on a one-minute schedule declaring `throughline_trace: Param(True)` produced two scheduled runs, both with `conf = {}` and **0 cells**; a manual trigger of the same DAG captured immediately. Param defaults are not written into a scheduled run's conf |
| `THROUGHLINE_TRACE_SCHEDULED` opts scheduled runs in | with it set and the containers restarted, a scheduled run of the same probe DAG captured; unset again, back to nothing |
| `trace_policy` opts one DAG's scheduled runs in, per DAG | two one-minute-schedule DAGs side by side with no environment variable set: the one declaring `trace_policy({"manual","scheduled"})` captured 8 cells over 2 scheduled runs, the control declaring nothing captured 0 |
| A nine-task DAG renders and scrolls | nine shape cards and ten table columns rendered; both the shape strip and the values table carry `overflow-x: auto`, and the field column is sticky so row labels survive scrolling right |
| `tools/check_demo.py` survives a missing git | run with `git` removed from PATH, it completes the seed and current-code checks, skips the bundle-v1 replay with a message naming the reason, and exits 0 — rather than raising `FileNotFoundError` out of a passing run |
| Path A runs on Windows, outside WSL | `tools/prove_isolation.py` run from a Windows checkout under a `.venv` reported 6/6 production writes blocked, scratch writes working and `wh.orders` unchanged at 5,000 rows. Invoked as `python`, not `python3` |
| Tracing does not touch the pipeline's own output | every warehouse table fingerprinted after a traced and an untraced run — 20 tables, identical row counts, columns and contents, no table or column added |
| Replay scopes on any SQL predicate | verified with a key equality, a non-key column, `IN (...)`, `BETWEEN`, and a two-column string predicate; the multi-record scopes captured every record and all were reachable from the replay's record list |
| A malformed scope fails loudly | `this is not sql` raised a parse error naming the predicate, and `wh.orders` was unchanged |
| The cost of tracing is known, not guessed | 0.36 s untraced against 1.38 s traced over the same work — about +1 s per 4,500 cells, ~14 bytes per cell on disk, a 2.4 ms Variable read per DAG parse, and literally nothing when the switch is off (`decorated is step`) |

## Found by running it in Airflow, and fixed

Both were silent. The DAG went green and captured nothing, which is the worst
shape a bug of this kind can take.

| Defect | Why no test caught it |
| --- | --- |
| **`run_type` never matched.** `dag_run.run_type` is a `DagRunType` enum inside a live task, and `str()` on it yields `"DagRunType.MANUAL"`, not `"manual"`, so switch 3 refused every run. Fixed in `throughline/runtime.py` by unwrapping the enum. | Importing `DagRunType` outside a task and stringifying it gives `"manual"`. The behaviour only differs on the live Task SDK object, so it is not reproducible off-scheduler |
| **The global switch could never be turned on.** `airflow.sdk.Variable` only works inside a running task; at DAG-parse time it raises `ImportError` on `SUPERVISOR_COMMS`, which the bare `except` turned into "off". Setting the Variable did nothing at all. Fixed in `throughline/config.py` by trying the metadata-DB accessor first. | The tests set `THROUGHLINE_ENABLED`, which is checked before Airflow is consulted, so they never exercised the Variable path |

## Assumptions that have now been settled

Every assumption previously listed here has been checked against a live 3.1.
One was wrong.

| Former assumption | Outcome |
| --- | --- |
| `context["dag_run"].bundle_version` is how a task reads its bundle version | **Wrong.** The attribute is absent under the `dags-folder` bundle, so versions record as `unknown` and the diff view falls back to labelling by run. It degrades exactly as designed — no run was harmed — but the attribute should not be relied on |
| TaskFlow renders `{{ params.throughline_scope }}` in arguments | **Correct**, shown above |
| `dag_run.conf` overrides a declared `param` of the same name | **Correct**, shown above |
| `@task` applied above `@throughline.trace` executes the wrapper in the worker | **Correct**, shown above |
| `Variable.get` works at DAG-parse time in 3.1 | **Wrong for the Task SDK accessor**, and the cause of the second defect above. The metadata-DB accessor does work at parse time |

## Found by running four DAGs at once, and fixed

| Defect | What happened |
| --- | --- |
| **A concurrent DAG died on the warehouse lock.** DuckDB takes an exclusive write lock per file, and under a LocalExecutor every task is its own process. Four DAGs triggered together raced, and the loser raised `IO Error: Could not set lock on file` rather than waiting. | `throughline/store.py` already retried lock conflicts for the capture store, but the task's own warehouse connection did not. The retry now lives in `throughline/locking.py` and both use it. It waits out lock conflicts only — a permission error or bad SQL still fails immediately |

## Found by clicking it, after the server said it was fine

| Defect | What happened |
| --- | --- |
| **Every link on the DAG tab did nothing.** They carried `target="_top"`, added so that opening a run would escape the iframe rather than nest Throughline inside itself. Airflow frames plugin pages with `sandbox="allow-scripts allow-same-origin allow-forms"`, which omits `allow-top-navigation`, so the browser refuses the navigation and reports nothing at all. | Every server-side check passed: the route returned 200, the HTML was correct, the plugin API advertised the view. Only a click showed it. The sandbox is hardcoded in Airflow's `ExternalView`, so the capability cannot be requested — links now navigate inside the frame, which keeps the DAG header and tab row visible anyway |

## Found by trying to switch it off

| Defect | What happened |
| --- | --- |
| **Turning the global switch off did nothing on a running Airflow.** Two consecutive runs captured 4,500 cells each with `throughline_enabled=false`. Not a lag: the second was 90 seconds later. | Not a bug in the switch so much as an undocumented consequence of its design. `@throughline.trace` is applied at *import*, and `include/orders_enrichment/steps.py` is imported early by the plugin's replay-plan module, so a long-lived process holds the already-decorated functions in `sys.modules`. A probe showed the contradiction directly: at parse time `enabled=False` while `wrapped=True`, and `importlib.reload` flipped it to `False`. Restarting applies the change in both directions. The README now says so |

## Environment requirements found the hard way

- **`throughline/` is baked into the image, not bind-mounted.** `astro dev`
  mounts `dags/`, `include/`, `plugins/` and `tests/`; a change to the plugin
  package itself needs `astro dev restart` before the containers see it. This
  is worth knowing because the failure is silent in the worst way: the
  containers keep running the previous version of the capture code, so tasks
  succeed and capture nothing. It cost an hour once already.
- **The bind-mounted DuckDB files must be writable by uid 50000.** The Astro
  containers run as `astro` (uid 50000); a file created on the host by an
  ordinary user is mode 644 and uid 1000, so the first task dies with
  `IO Error: ... Permission denied`. `include/scratch/` needs the same, or
  replay fails once it tries to create its scratch database. A `chown` in the
  Dockerfile does not help, because these are bind mounts rather than image
  content. `chmod -R a+rwX include` before `astro dev start` is the blunt fix.

## Still not verified

- **Anything beyond local `astro dev`.** No remote executor, no Astro
  deployment, no Kubernetes. Replay in particular runs in the API server
  process, which is a different proposition under a real deployment.
- **Wide parallel fan-out within one DAG.** Four *DAGs* in parallel is now
  covered (see above), and the lock retry in `throughline/locking.py` is what
  makes it work. What is still untested is many parallel tasks inside a single
  DAG, where contention is heavier than four writers.
- **`bundle_version` against real DAG bundle versioning.** Only the
  `dags-folder` bundle was exercised, which supplies no version at all.
- **Page rendering has no automated guard.** All four pages were confirmed by
  hand today, but nothing in `tests/`, `tools/check_demo.py` or CI asserts they
  render, so a broken template would still reach the camera silently.

## Known limitations

- **A broad scope replays the whole table.** `scope` is a raw SQL predicate
  with no width guard, so `1=1` re-executes every record — 5,000 on the demo
  warehouse. Production stays read-only throughout, so this costs time and
  capture-store space rather than data, but on an unauthenticated endpoint it
  is worth knowing.
- **Plugin endpoints are not auth-protected.** Airflow 3.1 does not
  authenticate `fastapi_apps` routes by default and this project does not add
  it. Confirmed directly: an unauthenticated request reads captured values and
  can trigger a replay. Fine for a demo; not fine for production without a
  proxy in front.
- **DuckDB only.** Tables are attached by file path and replay isolation uses
  `ATTACH ... (READ_ONLY)`. The equivalent on a real warehouse is a read-only
  role, described in the README, but no other warehouse is implemented.
- **Replay runs in the API server process**, not through the scheduler. That is
  what makes it take seconds, and it is why a DAG has to register its replay
  plan in `include/throughline_replays.py`.
- **The capture store is a single DuckDB file.** DuckDB takes an exclusive
  write lock per file, so concurrent tasks briefly contend. Writes are short and
  retried; a DAG with wide parallel fan-out would want Postgres instead.
