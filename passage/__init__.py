"""Passage — follow one record through a DAG.

Two uses, one mechanism. Reading four SQL files tells you what a pipeline is
supposed to do; watching one real order move through it tells you what it does.
The same capture answers "what does this DAG actually do to a record" and
"which task broke this particular one".

Public surface::

    @passage.trace(key="order_id")   # capture what a task received and returned
    passage.Table("orders", "wh")    # the handle tasks pass instead of the data
    passage.scope.resolve(...)       # Tier 2: narrow a source query to one record
"""

from __future__ import annotations

from passage import scope
from passage.errors import DecoratorOrderError, PassageError, ReplayRefused
from passage.registry import register_replay
from passage.table import Table, is_table, qualified
from passage.tracing import trace

__all__ = [
    "DecoratorOrderError",
    "PassageError",
    "ReplayRefused",
    "Table",
    "is_table",
    "qualified",
    "register_replay",
    "scope",
    "trace",
]

__version__ = "0.1.0"
