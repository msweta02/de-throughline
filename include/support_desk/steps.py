"""Task bodies for the three support-desk join DAGs.

These exist to answer the question ``orders_enrichment`` cannot: *does this
work on a DAG somebody else wrote, about something else entirely?* Nothing
here touches ``wh.orders`` or ``wh.promotions``. The domain is a support desk
— tickets, agents, queues and events — and the record key is ``ticket_id``,
not ``order_id``.

Between them they cover the three shapes a join pipeline takes:

* **first step** — the extract is itself a join across three tables.
* **every step** — a chain that widens the record one join at a time.
* **after a single-table extract** — one source, then a multi-table join.

``sd_with_events`` fans out for the handful of tickets that were reassigned
and so carry two events. A reassignment is a legitimate row, which makes this
a useful contrast with the promotions bug in ``orders_enrichment``: identical
shape in the grid, opposite conclusion.

One ticket is seeded with ``ticket_id = 88231``, the same number as the hero
*order*. Nothing joins the two and no trace should ever mix them; it is there
so that claim can be tested rather than asserted.
"""

from __future__ import annotations

import throughline
from throughline import session
from throughline.table import Table, qualified

# --------------------------------------------------------------------------
# Shape 1 — the join is the first step.
# --------------------------------------------------------------------------


@throughline.trace(key="ticket_id", replay_safe=True)
def jf_extract(scope: str = throughline.scope.ALL) -> Table:
    """Read tickets already joined to the agent and the queue."""
    predicate = throughline.scope.resolve(scope)
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_jf_extracted AS
            SELECT
                t.ticket_id,
                t.agent_id,
                t.queue_id,
                t.priority,
                t.opened_at,
                t.first_response_mins,
                a.agent_name,
                a.team,
                q.queue_name,
                q.sla_hours
            FROM wh.tickets t
            JOIN wh.agents a ON a.agent_id = t.agent_id
            JOIN wh.queues q ON q.queue_id = t.queue_id
            WHERE 1=1 AND {predicate}
            """
        )
    finally:
        con.close()
    return Table("sd_jf_extracted", target)


@throughline.trace(key="ticket_id", replay_safe=True)
def jf_normalize(tickets: Table) -> Table:
    """House style: hours rather than minutes, and drop the raw ids."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_jf_normalized AS
            SELECT
                ticket_id,
                agent_name,
                team,
                queue_name,
                sla_hours,
                priority,
                opened_at                        AS raised_at,
                first_response_mins / 60.0       AS first_response_hours
            FROM {qualified(tickets)}
            """
        )
    finally:
        con.close()
    return Table("sd_jf_normalized", target)


@throughline.trace(key="ticket_id", replay_safe=True)
def jf_score_sla(tickets: Table) -> Table:
    """Did the first response beat the queue's SLA, and by how much."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_jf_scored AS
            SELECT
                *,
                round(sla_hours - first_response_hours, 2) AS sla_headroom_hours,
                first_response_hours <= sla_hours          AS met_sla
            FROM {qualified(tickets)}
            """
        )
    finally:
        con.close()
    return Table("sd_jf_scored", target)


# --------------------------------------------------------------------------
# Shape 2 — a join at every step, widening the record one table at a time.
# --------------------------------------------------------------------------


@throughline.trace(key="ticket_id", replay_safe=True)
def es_with_agent(scope: str = throughline.scope.ALL) -> Table:
    """Tickets joined to agents. One row in, one row out."""
    predicate = throughline.scope.resolve(scope)
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_es_agent AS
            SELECT
                t.ticket_id,
                t.queue_id,
                t.priority,
                t.first_response_mins,
                a.agent_name,
                a.team,
                a.seniority
            FROM wh.tickets t
            JOIN wh.agents a ON a.agent_id = t.agent_id
            WHERE 1=1 AND {predicate}
            """
        )
    finally:
        con.close()
    return Table("sd_es_agent", target)


@throughline.trace(key="ticket_id", replay_safe=True)
def es_with_queue(tickets: Table) -> Table:
    """Add the queue. Still one row per ticket — queues are unique."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_es_queue AS
            SELECT
                t.ticket_id,
                t.priority,
                t.agent_name,
                t.team,
                t.seniority,
                t.first_response_mins / 60.0 AS first_response_hours,
                q.queue_name,
                q.sla_hours,
                q.severity_band
            FROM {qualified(tickets)} t
            JOIN wh.queues q ON q.queue_id = t.queue_id
            """
        )
    finally:
        con.close()
    return Table("sd_es_queue", target)


@throughline.trace(key="ticket_id", replay_safe=True)
def es_with_events(tickets: Table) -> Table:
    """Add ticket events — and this one fans out.

    A reassigned ticket has two events, so it leaves this task with two rows
    where one went in. Nothing is malformed: both rows are real. The shape
    strip shows the fan-out and names this task, and whether that is a problem
    depends on what the next task does with it — which is exactly the
    judgement the grid exists to support.
    """
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_es_events AS
            SELECT
                t.*,
                e.event_id,
                e.event_type,
                e.occurred_on
            FROM {qualified(tickets)} t
            LEFT JOIN wh.ticket_events e ON e.ticket_id = t.ticket_id
            """
        )
    finally:
        con.close()
    return Table("sd_es_events", target)


@throughline.trace(key="ticket_id", replay_safe=True)
def es_score_sla(tickets: Table) -> Table:
    """SLA headroom per row. A fanned-out ticket gets one per event."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_es_scored AS
            SELECT
                *,
                round(sla_hours - first_response_hours, 2) AS sla_headroom_hours,
                first_response_hours <= sla_hours          AS met_sla
            FROM {qualified(tickets)}
            """
        )
    finally:
        con.close()
    return Table("sd_es_scored", target)


# --------------------------------------------------------------------------
# Shape 3 — a single-table extract, then one step joining several tables.
# --------------------------------------------------------------------------


@throughline.trace(key="ticket_id", replay_safe=True)
def as_extract(scope: str = throughline.scope.ALL) -> Table:
    """One source table, nothing joined yet."""
    predicate = throughline.scope.resolve(scope)
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_as_extracted AS
            SELECT ticket_id, agent_id, queue_id, priority, first_response_mins
            FROM wh.tickets
            WHERE 1=1 AND {predicate}
            """
        )
    finally:
        con.close()
    return Table("sd_as_extracted", target)


@throughline.trace(key="ticket_id", replay_safe=True)
def as_join_reference(tickets: Table) -> Table:
    """Agents and queues joined in a single step.

    No ``{scope}`` here: this task reads source tables, but it reaches them
    through a join to an already-scoped input, so it inherits the narrowing.
    """
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_as_joined AS
            SELECT
                t.ticket_id,
                t.priority,
                t.first_response_mins / 60.0 AS first_response_hours,
                a.agent_name,
                a.team,
                a.seniority,
                q.queue_name,
                q.sla_hours,
                q.severity_band
            FROM {qualified(tickets)} t
            JOIN wh.agents a ON a.agent_id = t.agent_id
            JOIN wh.queues q ON q.queue_id = t.queue_id
            """
        )
    finally:
        con.close()
    return Table("sd_as_joined", target)


@throughline.trace(key="ticket_id", replay_safe=True)
def as_score_sla(tickets: Table) -> Table:
    """SLA headroom, and how far the response sat inside the severity band."""
    target = session.write_target()
    con = session.connect()
    try:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target}.sd_as_scored AS
            SELECT
                *,
                round(sla_hours - first_response_hours, 2)                AS sla_headroom_hours,
                first_response_hours <= sla_hours                         AS met_sla,
                round(first_response_hours / sla_hours * 100, 1)          AS sla_used_pct
            FROM {qualified(tickets)}
            """
        )
    finally:
        con.close()
    return Table("sd_as_scored", target)
