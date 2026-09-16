# What has been verified, and what has not

Airflow 3.1 is recent and its plugin and context APIs are easy to hallucinate.
This file records exactly which claims in this repo have been executed and which
are still assumptions, so that a reviewer does not have to guess.

**The honest headline: no part of this has run inside an Airflow scheduler yet.**
Docker was unavailable in the development environment, so everything Airflow-facing
is either copied from a plugin known to run on Astro Runtime 3.1-1 or is marked
below as unverified. Everything *not* Airflow-facing — capture, the store, the
grid, replay isolation — has been run end to end and is covered by tests.

## Verified by execution

| Claim | How |
| --- | --- |
| Replay cannot write to production | `tools/prove_isolation.py` — six kinds of write against a `READ_ONLY` attached database, all rejected by DuckDB, with row counts unchanged |
| Scratch writes still work during replay | same script |
| The seeded bug fans out for exactly 3 of 5,000 customers | `tools/seed_warehouse.py`, then a count of orders with more than one output row |
| `1 -> 1 -> 2 -> 2` for record 88231 under bundle-v1 | `tools/local_run.py --replay --scope "order_id = 88231"` |
| `1 -> 1 -> 1 -> 1` for the same record under bundle-v2 | same, after the fix |
| `row_ordinal` keeps both rows of a fanned-out record | `tests/test_passage.py` |
| The global switch removes the wrapper rather than short-circuiting it | `tests/test_passage.py` — asserts the returned object *is* the original function |
| Scheduled runs capture nothing by default | `tests/test_passage.py` |
| Sampling caps by record, not by row | `tests/test_passage.py` |
| Tracing does not change what a task returns | `tests/test_passage.py` |
| Replay refuses tasks that are not marked safe, by name | `tests/test_passage.py` |
| All four pages render | rendered to HTML from real captures, asserting the grid and diff text |
| A full traced run finds all 3 broken records unaided | `tools/local_run.py --sample all` over 5,000 orders, ~8s |
| Captured values are escaped safely on the bulk-insert path | `tests/test_passage.py` — a value containing `'); DROP TABLE ...` round-trips intact and the store survives |
| CLI and plugin replays take the same path | `tools/local_run.py --replay` calls `passage.replay.run`, so both hit the same preflight and both appear in the replays table |

## Verified by copying something that works

`AirflowPlugin` with `fastapi_apps` and `external_views` — the attribute shape in
`plugins/passage_plugin.py` is taken from a plugin running against Astro Runtime
3.1-1. That includes the gotcha that `external_views["href"]` must be relative,
without a leading slash, and must agree with `fastapi_apps["url_prefix"]`, or
RBAC denies access to the page. Here both are derived from one constant so they
cannot drift.

## Not yet verified — check these first when Airflow is available

| Assumption | Where | If it is wrong |
| --- | --- | --- |
| `context["dag_run"].bundle_version` is how a task reads its bundle version | `passage/runtime.py` | Versions record as `unknown`; the diff view loses its labels but still diffs by run. Falls back to `PASSAGE_BUNDLE_VERSION` |
| TaskFlow renders `{{ params.passage_scope }}` in arguments passed to a task | `dags/orders_enrichment.py` | Scoping falls back to the predicate on `dag_run.conf`, which `passage/scope.py` already reads. This is why that fallback exists |
| `dag_run.conf` overrides a declared `param` of the same name at trigger time | replay triggering | Same fallback covers it |
| `@task` applied above `@passage.trace` executes the wrapper in the worker | `passage/tracing.py` | Capture would run at parse time. The reverse order raises `DecoratorOrderError`, so the failure is loud either way |
| `Variable.get` works at DAG-parse time in 3.1 | `passage/config.py` | Global switch reads as off and nothing captures. `PASSAGE_ENABLED` is checked first and bypasses Airflow entirely |

Each of these degrades to something harmless rather than raising, which is
deliberate: a tracing tool that breaks the pipeline it is observing has failed
at its job.

## Known limitations

- **Plugin endpoints are not auth-protected.** Airflow 3.1 does not
  authenticate `fastapi_apps` routes by default and this project does not add
  it. Anyone who can reach the API server can read captured values and trigger
  a replay. Fine for a demo; not fine for production without a proxy in front.
- **DuckDB only.** Tables are attached by file path and replay isolation uses
  `ATTACH ... (READ_ONLY)`. The equivalent on a real warehouse is a read-only
  role, described in the README, but no other warehouse is implemented.
- **Replay runs in the API server process**, not through the scheduler. That is
  what makes it take seconds, and it is why a DAG has to register its replay
  plan in `include/passage_replays.py`.
- **The capture store is a single DuckDB file.** DuckDB takes an exclusive
  write lock per file, so concurrent tasks briefly contend. Writes are short and
  retried; a DAG with wide parallel fan-out would want Postgres instead.
