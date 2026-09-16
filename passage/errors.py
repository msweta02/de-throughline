"""Failure modes Passage raises on purpose."""

from __future__ import annotations


class PassageError(Exception):
    """Base class, so a DAG can catch everything Passage raises."""


class ReplayRefused(PassageError):
    """A replay touched a task that was never marked safe to re-execute.

    Refusing is the correct default. The downside of replaying a task that was
    not written to be replayed is a corrupted production table, and no grid is
    worth that.
    """


class DecoratorOrderError(PassageError):
    """``@passage.trace`` was applied above ``@task`` instead of below it."""
