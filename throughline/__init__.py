"""Throughline — follow one record through a DAG.

Two uses, one mechanism. Reading four SQL files tells you what a pipeline is
supposed to do; watching one real order move through it tells you what it does.
The same capture answers "what does this DAG actually do to a record" and
"which task broke this particular one".

Public surface::

    @throughline.trace(key="order_id")   # capture what a task received and returned
    throughline.Table("orders", "wh")    # the handle tasks pass instead of the data
    throughline.scope.resolve(...)       # Tier 2: narrow a source query to one record
"""

from __future__ import annotations

from throughline import scope
from throughline.errors import DecoratorOrderError, ReplayRefused, ThroughlineError
from throughline.registry import register_replay
from throughline.table import Table, is_table, qualified
from throughline.tracing import trace

__all__ = [
    "DecoratorOrderError",
    "ThroughlineError",
    "ReplayRefused",
    "Table",
    "is_table",
    "qualified",
    "register_replay",
    "scope",
    "trace",
]

__version__ = "0.1.0"
