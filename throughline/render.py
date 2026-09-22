"""Server-rendered HTML for the trace view.

Deliberately Jinja and a single ``<style>`` block: no React, no build step, no
bundle to serve. Airflow 3.1's React plugin path is experimental, and the view
here is a table — the whole point is that it is legible at a glance, which is
not a problem a front-end framework solves.
"""

from __future__ import annotations

from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from throughline import paths

_TEMPLATES = paths.PROJECT_ROOT / "throughline" / "templates"

_env = Environment(
    loader=FileSystemLoader(str(_TEMPLATES)),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
)


def page(name: str, **context: Any) -> str:
    return _env.get_template(name).render(**context)
