"""Turning whatever crossed a task boundary into rows.

Throughline captures anything it can read as records: a warehouse relation behind a
:class:`~throughline.table.Table` handle, a list of dicts, a single dict, or any
object exposing ``to_dict("records")`` (which covers pandas and polars without
importing either). Anything else is left alone and recorded as uncapturable —
a task returning a model object or a file path is not an error, it just has
nothing record-shaped to show.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from throughline import table as table_mod


@dataclass
class Snapshot:
    """One side of one task boundary, flattened to rows."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    kind: str = "none"
    source: str | None = None

    @property
    def captured(self) -> bool:
        return self.kind != "none"


def to_text(value: Any) -> str | None:
    """Render a warehouse value as text, keeping NULL distinct from ``"None"``."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float, Decimal, dt.date, dt.datetime, dt.time)):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (list, tuple, dict)):
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def _rows_from_relation(
    con: Any, qualified: str, key: str | None, sample: int | None
) -> list[dict]:
    """Read a warehouse relation, capped at ``sample`` distinct record keys.

    The cap is pushed into SQL rather than applied after fetching, so tracing a
    task over a large table does not drag the whole table into memory first.
    """
    if key and sample:
        # Whole records, not whole rows: a record that fans out to two rows
        # must bring both of them, or the fan-out is invisible.
        sql = (
            f'SELECT * FROM {qualified} WHERE "{key}" IN '
            f'(SELECT "{key}" FROM {qualified} GROUP BY "{key}" ORDER BY "{key}" LIMIT {int(sample)}) '
            f'ORDER BY "{key}"'
        )
    elif key:
        sql = f'SELECT * FROM {qualified} ORDER BY "{key}"'
    elif sample:
        sql = f"SELECT * FROM {qualified} LIMIT {int(sample)}"
    else:
        sql = f"SELECT * FROM {qualified}"

    cursor = con.execute(sql)
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _cap_records(rows: list[dict], key: str | None, sample: int | None) -> list[dict]:
    """Apply the sampling cap to in-memory rows, by record rather than by row."""
    if not sample or len(rows) <= sample:
        return rows
    if not key:
        return rows[:sample]
    keep: list[dict] = []
    seen: set[Any] = set()
    for row in rows:
        value = row.get(key)
        if value not in seen:
            if len(seen) >= sample:
                continue
            seen.add(value)
        keep.append(row)
    return keep


def take(
    value: Any, *, key: str | None = None, sample: int | None = None, con: Any = None
) -> Snapshot:
    """Snapshot one value. Never raises: capture must not break the task."""
    if value is None:
        return Snapshot(kind="none")

    try:
        if table_mod.is_table(value):
            qualified = table_mod.qualified(value)
            if con is None:
                return Snapshot(kind="none", source=qualified)
            return Snapshot(_rows_from_relation(con, qualified, key, sample), "table", qualified)

        if isinstance(value, dict):
            return Snapshot([value], "dict")

        if isinstance(value, (list, tuple)) and value and all(isinstance(r, dict) for r in value):
            return Snapshot(_cap_records(list(value), key, sample), "rows")

        # pandas / polars, duck-typed so neither becomes a dependency.
        to_dict = getattr(value, "to_dict", None)
        if callable(to_dict):
            records = to_dict("records")
            if isinstance(records, list):
                return Snapshot(_cap_records(records, key, sample), "dataframe")
    except Exception:
        # A snapshot that fails is a missing column in the grid, not a failed task.
        return Snapshot(kind="none")

    return Snapshot(kind="none")


def flatten(snapshot: Snapshot, key: str | None) -> list[tuple[str | None, int, str, str | None]]:
    """Explode rows into ``(record_key, row_ordinal, field_name, value)``.

    ``row_ordinal`` counts *within* a record key, which is what makes a fan-out
    legible: one key, ordinals 0 and 1, two rows where there used to be one.
    """
    ordinals: dict[str | None, int] = {}
    out: list[tuple[str | None, int, str, str | None]] = []
    for row in snapshot.rows:
        record_key = to_text(row.get(key)) if key else None
        ordinal = ordinals.get(record_key, 0)
        ordinals[record_key] = ordinal + 1
        for field_name, value in row.items():
            out.append((record_key, ordinal, str(field_name), to_text(value)))
    return out
