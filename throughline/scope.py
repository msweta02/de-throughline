"""Tier 2 adoption: narrowing a source query to one record.

The convention is one line per source query::

    select * from orders where 1=1 and {{ params.throughline_scope }}

which renders to ``true`` on a normal run and to ``order_id = 88231`` during a
scoped replay. It is only needed on tasks that read source tables, and only if
you want replay to touch one record instead of the whole table.

There is deliberately no automatic version of this. Injecting a predicate into
arbitrary SQL means parsing and rewriting it, which is a research project, not
a feature. One line you can read is more honest than a rewriter you cannot.
"""

from __future__ import annotations

from throughline import runtime

#: What an unscoped run narrows to: everything.
ALL = "true"


def resolve(value: str | None = None) -> str:
    """Work out the predicate this task should filter its source query with.

    Prefers the templated value the DAG passed in, and falls back to the scope
    carried on ``dag_run.conf``. The fallback is what makes the templating a
    convenience rather than a dependency: if ``{{ params.throughline_scope }}``
    turns out not to render where this design assumes, a scoped replay still
    scopes, because the predicate is on the conf either way.
    """
    if value:
        text = str(value).strip()
        # An unrendered template means Jinja never ran on this argument.
        if text and "{{" not in text:
            return text

    return runtime.current().scope or ALL
