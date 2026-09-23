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
| A full traced run finds all 3 broken records unaided | `tools/local_run.py --sample all` over 5,000 orders, ~13s on the development machine |
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
| Tracing works on a DAG that joins | `orders_join_every_step` captured 100 records / 6,434 cells across 4 tasks, unchanged decorator |
| A join fan-out surfaces the same as the seeded bug | order 83245 (two shipments) read `1 -> 1 -> 2 -> 2`, attributed to `with_shipment` |
| The record list names its real key column | the header reads `order_id`, read from the captures rather than configured |
| Filtering the record list works | `?q=83245` returned *1 of 100 records*; a non-matching filter says so rather than rendering an empty table |

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
- **Concurrency.** Every run tested here was sequential. The capture store is a
  single DuckDB file with an exclusive write lock, so a DAG with wide parallel
  fan-out is exactly the case not covered.
- **`bundle_version` against real DAG bundle versioning.** Only the
  `dags-folder` bundle was exercised, which supplies no version at all.
- **Page rendering has no automated guard.** All four pages were confirmed by
  hand today, but nothing in `tests/`, `tools/check_demo.py` or CI asserts they
  render, so a broken template would still reach the camera silently.

## Known limitations

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
