# Contributing

This is a hackathon build, so the bar for a change is narrow: **does it make one
of the beats in [docs/demo-script.md](docs/demo-script.md) work better?** If not,
it is out of scope — see the non-goals in [CLAUDE.md](CLAUDE.md).

## Getting set up

```bash
pip install -r requirements-dev.txt
python3 tools/seed_warehouse.py
python3 tools/local_run.py --run-id local
```

No Airflow needed for any of that. The task bodies live in
`include/orders_enrichment/steps.py` precisely so they can be run without one.

## Before you open a pull request

```bash
ruff check . && ruff format --check .
pytest tests/ -q
python3 tools/prove_isolation.py
python3 tools/check_demo.py
```

CI runs exactly these on 3.11, 3.12 and 3.13.

## Invariants

These are the things whose failure would be silent, which is why they are
listed rather than left to review:

- **The global switch removes the wrapper**, it does not short-circuit inside
  one. The test asserts object identity. If it ever starts asserting behaviour
  instead, the guarantee has quietly been lost.
- **`row_ordinal` counts within a record key.** Collapsing it hides the
  fan-out, which is the entire demo.
- **Capture never raises into the task.** A tracing tool that breaks the
  pipeline it observes has failed at its job. Snapshot failures log and swallow.
- **`@passage.trace` goes below `@task`.** The reverse runs at parse time.
- **Captured data never goes through XCom.**
- **Passage writes only to `include/passage.duckdb`.** Its observer connection
  attaches the warehouse `READ_ONLY`.
- **Replay stays refused by default.** Tasks opt in with `replay_safe=True`.

`tools/check_demo.py` guards the numbers quoted in the README. If you change
the seed, the pipeline or the fix, expect it to fail and update the docs in the
same commit.

## Airflow-facing changes

Read [VERIFY.md](VERIFY.md) first. Nothing here has run inside an Airflow
scheduler yet, and `passage/runtime.py` is deliberately the only module that
touches Airflow, so that a wrong guess about 3.1 is a one-file fix. Keep it
that way: if you need a new piece of Airflow context, add an accessor there
with a fallback rather than importing Airflow somewhere new.
