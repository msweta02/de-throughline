"""Which databases a task can read, and which it can write.

This is where replay safety is made mechanical rather than conventional. A
sandbox schema only protects production if every task remembers to parameterise
its write target, and one hardcoded ``INSERT INTO analytics.orders_enriched``
undoes it silently. So during a replay the warehouse is attached ``READ_ONLY``
and writes go to a scratch database: a task that writes to production raises
immediately instead of succeeding quietly.

The warehouse-general equivalent, for anyone not on DuckDB, is in the README:
run replay under a role with read-only grants on production and write access
only to a scratch schema. Same guarantee, enforced by the warehouse either way.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from throughline import paths, runtime
from throughline.table import SCRATCH_ALIAS, WAREHOUSE_ALIAS

if TYPE_CHECKING:  # pragma: no cover
    import duckdb


def connect(rt: runtime.Runtime | None = None) -> duckdb.DuckDBPyConnection:
    """Open a connection configured for whatever mode this task is running in.

    Normal run: the warehouse is attached read-write and is the write target.
    Replay: the warehouse is attached read-only and a per-replay scratch
    database takes the writes.
    """
    import duckdb

    rt = rt or runtime.current()
    con = duckdb.connect(":memory:")

    warehouse = paths.warehouse_db()
    warehouse.parent.mkdir(parents=True, exist_ok=True)

    if rt.is_replay:
        # READ_ONLY is the whole guarantee. Everything else is bookkeeping.
        con.execute(f"ATTACH '{warehouse}' AS {WAREHOUSE_ALIAS} (READ_ONLY)")
        scratch = paths.scratch_db(rt.replay_id or rt.run_id)
        con.execute(f"ATTACH '{scratch}' AS {SCRATCH_ALIAS}")
    else:
        con.execute(f"ATTACH '{warehouse}' AS {WAREHOUSE_ALIAS}")

    return con


def write_target(rt: runtime.Runtime | None = None) -> str:
    """The catalog a task should create its output in.

    Tasks interpolate this into their DDL. A task that hardcodes ``wh`` instead
    still runs normally and still fails loudly under replay, which is the
    intended outcome — see the refusal demo in the README.
    """
    rt = rt or runtime.current()
    return SCRATCH_ALIAS if rt.is_replay else WAREHOUSE_ALIAS


def connect_observer(rt: runtime.Runtime | None = None) -> duckdb.DuckDBPyConnection:
    """The connection Throughline snapshots through. Read-only on everything.

    Throughline never writes to the warehouse, so it never asks for write access to
    it. That is partly principle and partly mechanics: the task has just been
    writing through its own connection, and a second read-write attachment
    would be contending for DuckDB's exclusive write lock for no reason.
    """
    import duckdb

    rt = rt or runtime.current()
    con = duckdb.connect(":memory:")

    warehouse = paths.warehouse_db()
    if warehouse.exists():
        con.execute(f"ATTACH '{warehouse}' AS {WAREHOUSE_ALIAS} (READ_ONLY)")

    if rt.is_replay:
        scratch = paths.scratch_db(rt.replay_id or rt.run_id)
        if scratch.exists():
            con.execute(f"ATTACH '{scratch}' AS {SCRATCH_ALIAS} (READ_ONLY)")

    return con
