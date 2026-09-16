"""The handle tasks pass to each other instead of the data itself.

A task hands downstream a pointer to a warehouse relation, not its rows. That
keeps XCom small — XCom is for control flow, never for captured data — while
still giving ``@passage.trace`` something it can turn into rows on both sides
of the task boundary.

``Table`` subclasses ``dict`` on purpose. Whatever XCom serializer is in play,
a dict survives the round trip, and it comes back as a plain dict that
:func:`is_table` still recognises by its marker key.
"""

from __future__ import annotations

from typing import Any

MARKER = "__passage_table__"

#: Attached catalog names. Task SQL is written against these, so the same SQL
#: runs in a normal run and in a replay with different things underneath.
WAREHOUSE_ALIAS = "wh"
SCRATCH_ALIAS = "scratch"


class Table(dict):
    """A pointer to ``<database>.<name>`` in the currently attached catalogs."""

    def __init__(self, name: str, database: str = WAREHOUSE_ALIAS) -> None:
        super().__init__(**{MARKER: True, "name": name, "database": database})

    @property
    def name(self) -> str:
        return str(self["name"])

    @property
    def database(self) -> str:
        return str(self["database"])

    @property
    def qualified(self) -> str:
        return f"{self.database}.{self.name}"

    def __str__(self) -> str:  # pragma: no cover - convenience only
        return self.qualified


def is_table(value: Any) -> bool:
    """True for a ``Table`` or for the plain dict one decays into via XCom."""
    return isinstance(value, dict) and bool(value.get(MARKER))


def qualified(value: Any) -> str:
    """The ``database.name`` of a handle, whether or not it survived as a class."""
    return f"{value['database']}.{value['name']}"
