"""The capture table: one row per field, per row, per side, per task.

Long format rather than one row per record. It costs storage and buys two
things worth more than the storage:

* the grid is a pivot, not a bespoke assembly step, and
* comparing two bundle versions is ``where bundle_version in (...)`` rather
  than a second code path.

``row_ordinal`` is the load-bearing column. One record key can map to several
rows — that is the entire point of the fan-out case — so anything keyed only by
``record_key`` would collapse exactly the evidence the tool exists to show.

The store is its own DuckDB file. Throughline writes here and nowhere else.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

from throughline import paths, runtime
from throughline import snapshot as snapshot_mod

# The capture *file* is already Throughline's, so the schema inside it is named for
# what it holds. Calling it "throughline" would collide with DuckDB's catalog name
# for throughline.duckdb and make every reference ambiguous.
SCHEMA = "capture"

_CAPTURES_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};
CREATE TABLE IF NOT EXISTS {SCHEMA}.captures (
    capture_id      VARCHAR,
    dag_id          VARCHAR,
    run_id          VARCHAR,
    bundle_version  VARCHAR,
    task_id         VARCHAR,
    direction       VARCHAR,   -- 'in' | 'out'
    record_key      VARCHAR,
    row_ordinal     INTEGER,
    field_name      VARCHAR,
    value           VARCHAR,
    captured_at     TIMESTAMP
);
CREATE TABLE IF NOT EXISTS {SCHEMA}.replays (
    replay_id       VARCHAR,
    dag_id          VARCHAR,
    source_run_id   VARCHAR,
    bundle_version  VARCHAR,
    record_key      VARCHAR,
    scope           VARCHAR,
    query           VARCHAR,
    status          VARCHAR,
    note            VARCHAR,
    is_regression   BOOLEAN,
    created_at      TIMESTAMP
);
"""

#: DuckDB holds an exclusive write lock per file. Tasks in the same DAG run are
#: separate processes under LocalExecutor, so a brief collision is possible even
#: in a linear DAG. Connections are held for the length of one insert and
#: retried, which is cheaper than standing up a second database for captures.
_LOCK_RETRIES = 12
_LOCK_BACKOFF = 0.25


def _is_lock_conflict(exc: Exception) -> bool:
    """Whether an exception is another process holding the write lock."""
    text = str(exc).lower()
    return "lock" in text or "being used by another" in text or "conflict" in text


def connect(read_only: bool = False) -> Any:
    """Open the capture store, creating it on first use."""
    import duckdb

    path = paths.capture_db()
    path.parent.mkdir(parents=True, exist_ok=True)

    last: Exception | None = None
    for attempt in range(_LOCK_RETRIES):
        try:
            con = duckdb.connect(str(path), read_only=read_only and path.exists())
            if not read_only:
                con.execute(_CAPTURES_DDL)
            return con
        except Exception as exc:
            # Only lock contention is worth waiting out. Retrying a schema or
            # permission error just hides it behind twenty seconds of backoff.
            if not _is_lock_conflict(exc):
                raise
            last = exc
            time.sleep(_LOCK_BACKOFF * (attempt + 1))
    raise RuntimeError(f"could not open the Throughline capture store at {path}") from last


def record(
    rt: runtime.Runtime,
    direction: str,
    snap: snapshot_mod.Snapshot,
    key: str | None,
) -> int:
    """Write one side of one task boundary. Returns the number of rows written."""
    cells = snapshot_mod.flatten(snap, key)
    if not cells:
        return 0

    capture_id = str(uuid.uuid4())
    now = datetime.now(UTC)
    payload = [
        (
            capture_id,
            rt.dag_id,
            rt.run_id,
            rt.bundle_version,
            rt.task_id,
            direction,
            record_key,
            ordinal,
            field_name,
            value,
            now,
        )
        for record_key, ordinal, field_name, value in cells
    ]

    con = connect()
    try:
        _insert(con, payload)
    finally:
        con.close()
    return len(payload)


#: Rows per INSERT statement on the fast path.
_CHUNK = 5000


def _literal(value: Any) -> str:
    """Render one value as a SQL literal, escaped.

    Doubling single quotes is the complete escape for a DuckDB string literal,
    and NUL is stripped because a varchar cannot hold one. Everything written
    here is already text from :func:`throughline.snapshot.to_text`.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("\x00", "").replace("'", "''") + "'"


def _insert(con: Any, payload: list[tuple]) -> None:
    """Write a batch of cells.

    Literals are inlined rather than bound as parameters because DuckDB binds
    them one at a time: 45,000 cells takes 60 seconds through ``executemany``
    and under a second this way. The capture store is Throughline's own private
    database and every value has already been stringified, but the escaping
    above is still exact — and if it ever is not, the parameterised path below
    runs instead, after a rollback so nothing lands twice.
    """
    try:
        con.execute("BEGIN TRANSACTION")
        for start in range(0, len(payload), _CHUNK):
            chunk = payload[start : start + _CHUNK]
            values = ",".join(
                "(" + ",".join(_literal(value) for value in row) + ")" for row in chunk
            )
            con.execute(f"INSERT INTO {SCHEMA}.captures VALUES {values}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        con.executemany(f"INSERT INTO {SCHEMA}.captures VALUES (?,?,?,?,?,?,?,?,?,?,?)", payload)


def _rows(con: Any, sql: str, params: list | None = None) -> list[dict]:
    cursor = con.execute(sql, params or [])
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def list_traces(limit: int = 50) -> list[dict]:
    """One entry per captured DAG run, newest first."""
    con = connect(read_only=True)
    try:
        return _rows(
            con,
            f"""
            SELECT dag_id, run_id, any_value(bundle_version) AS bundle_version,
                   count(DISTINCT task_id) AS tasks,
                   count(DISTINCT record_key) AS records,
                   min(captured_at) AS started_at
            FROM {SCHEMA}.captures
            GROUP BY dag_id, run_id
            ORDER BY started_at DESC
            LIMIT {int(limit)}
            """,
        )
    finally:
        con.close()


def list_records(dag_id: str, run_id: str, limit: int = 200) -> list[dict]:
    """The records captured in a run, with the row count each ended up at."""
    con = connect(read_only=True)
    try:
        return _rows(
            con,
            f"""
            WITH final AS (
                SELECT record_key, task_id, max(row_ordinal) + 1 AS rows
                FROM {SCHEMA}.captures
                WHERE dag_id = ? AND run_id = ? AND direction = 'out'
                GROUP BY record_key, task_id
            )
            SELECT record_key, max(rows) AS max_rows, min(rows) AS min_rows
            FROM final
            WHERE record_key IS NOT NULL
            GROUP BY record_key
            ORDER BY max_rows DESC, record_key
            LIMIT {int(limit)}
            """,
            [dag_id, run_id],
        )
    finally:
        con.close()


def count_records(dag_id: str, run_id: str) -> int:
    """How many distinct records a run captured, ignoring any listing limit."""
    con = connect(read_only=True)
    try:
        return int(
            con.execute(
                f"SELECT count(DISTINCT record_key) FROM {SCHEMA}.captures "
                f"WHERE dag_id = ? AND run_id = ? AND record_key IS NOT NULL",
                [dag_id, run_id],
            ).fetchone()[0]
        )
    finally:
        con.close()


def captures_for(dag_id: str, run_id: str, record_key: str) -> list[dict]:
    """Every captured cell for one record in one run."""
    con = connect(read_only=True)
    try:
        return _rows(
            con,
            f"""
            SELECT task_id, direction, row_ordinal, field_name, value,
                   bundle_version, captured_at
            FROM {SCHEMA}.captures
            WHERE dag_id = ? AND run_id = ? AND record_key = ?
            ORDER BY captured_at, row_ordinal
            """,
            [dag_id, run_id, record_key],
        )
    finally:
        con.close()


def task_order(dag_id: str, run_id: str) -> list[str]:
    """Tasks in the order they first captured anything — the grid's columns."""
    con = connect(read_only=True)
    try:
        return [
            r["task_id"]
            for r in _rows(
                con,
                f"""
                SELECT task_id, min(captured_at) AS first_seen
                FROM {SCHEMA}.captures
                WHERE dag_id = ? AND run_id = ?
                GROUP BY task_id ORDER BY first_seen
                """,
                [dag_id, run_id],
            )
        ]
    finally:
        con.close()


def runs_for_record(dag_id: str, record_key: str) -> list[dict]:
    """Every run that captured this record — the candidates for a diff."""
    con = connect(read_only=True)
    try:
        return _rows(
            con,
            f"""
            SELECT run_id, any_value(bundle_version) AS bundle_version,
                   min(captured_at) AS started_at
            FROM {SCHEMA}.captures
            WHERE dag_id = ? AND record_key = ?
            GROUP BY run_id ORDER BY started_at DESC
            """,
            [dag_id, record_key],
        )
    finally:
        con.close()


def save_replay(entry: dict) -> None:
    """Record a replay so it can be listed, and later re-run as a regression test."""
    con = connect()
    try:
        con.execute(
            f"INSERT INTO {SCHEMA}.replays VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                entry.get("replay_id"),
                entry.get("dag_id"),
                entry.get("source_run_id"),
                entry.get("bundle_version"),
                entry.get("record_key"),
                entry.get("scope"),
                entry.get("query"),
                entry.get("status"),
                entry.get("note"),
                bool(entry.get("is_regression")),
                datetime.now(UTC),
            ],
        )
    finally:
        con.close()


def list_replays(limit: int = 50) -> list[dict]:
    con = connect(read_only=True)
    try:
        return _rows(
            con, f"SELECT * FROM {SCHEMA}.replays ORDER BY created_at DESC LIMIT {int(limit)}"
        )
    finally:
        con.close()
