"""Where Passage keeps its files.

Two DuckDB databases, deliberately separate files:

* ``warehouse.duckdb`` — the pipeline's own tables. Passage never writes here.
* ``passage.duckdb``   — the capture store. Passage only writes here.

Keeping the capture store in its own file (rather than a schema inside the
warehouse) is the strongest available version of "Passage is a pure observer":
during a replay the warehouse is attached ``READ_ONLY`` and the capture store
still takes writes, because it was never part of that database to begin with.
"""

from __future__ import annotations

import os
from pathlib import Path

# ``include/`` in an Astro project — this file is at ``passage/paths.py``.
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _base() -> Path:
    """Root for all Passage data. ``PASSAGE_HOME`` lets tests redirect it."""
    return Path(os.environ.get("PASSAGE_HOME", PROJECT_ROOT / "include"))


def warehouse_db() -> Path:
    """The pipeline's warehouse. Production, as far as this demo is concerned."""
    return _base() / "warehouse.duckdb"


def capture_db() -> Path:
    """The capture store. Passage's own database, never the pipeline's."""
    return _base() / "passage.duckdb"


def scratch_db(replay_id: str) -> Path:
    """A throwaway database for one replay's writes."""
    path = _base() / "scratch" / f"{replay_id}.duckdb"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
