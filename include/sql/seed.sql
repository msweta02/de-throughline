-- The demo warehouse: 5,000 customers, one order each, and a promotions table
-- whose validity windows overlap for exactly three of them.
--
-- Everything is derived from the row number rather than from random(), so the
-- same seed produces the same warehouse on every machine — including the
-- hero record, order 88231.

CREATE OR REPLACE TABLE wh.orders AS
SELECT
    83231 + i                                              AS order_id,
    i                                                      AS customer_id,
    TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i % 28) DAY
                                     + INTERVAL (i % 24) HOUR
                                                           AS order_ts,
    ['SKU-ALPHA','SKU-BRAVO','SKU-CIRRUS','SKU-DELTA','SKU-ECHO'][1 + (i % 5)]
                                                           AS sku,
    1 + (i % 5)                                            AS qty,
    500 + ((i * 37) % 19500)                               AS unit_price_cents,
    'USD'                                                  AS currency,
    ['web','mobile','partner'][1 + (i % 3)]                AS channel
FROM range(1, 5001) t(i);

-- One promo per customer, valid across the whole order window.
CREATE OR REPLACE TABLE wh.promotions AS
SELECT
    100000 + i                     AS promo_id,
    i                              AS customer_id,
    'AUTUMN-' || (5 + (i % 3) * 5) AS promo_code,
    5 + (i % 3) * 5                AS discount_pct,   -- 5, 10 or 15
    DATE '2026-08-15'              AS valid_from,
    DATE '2026-10-15'              AS valid_to
FROM range(1, 5001) t(i);

-- The seeded defect. Three customers get a *second* promo whose window overlaps
-- the first. Nothing about these rows is malformed: no nulls, no bad types, no
-- duplicate promo_id. They are simply two valid promotions at the same time,
-- which is a thing a real promotions table does.
--
-- Three out of five thousand is the point. A bug that broke every row would
-- show up in any aggregate and would not need a record-scoped tool to find.
INSERT INTO wh.promotions VALUES
    (900001, 1204, 'LOYALTY-20', 20, DATE '2026-09-01', DATE '2026-09-30'),
    (900002, 3877, 'LOYALTY-20', 20, DATE '2026-09-01', DATE '2026-09-30'),
    (900003, 5000, 'LOYALTY-20', 20, DATE '2026-09-01', DATE '2026-09-30');


-- ---------------------------------------------------------------------------
-- A second, unrelated domain: the support desk.
--
-- The join DAGs deliberately do not touch orders. A tool that only works on
-- the pipeline it was written against proves nothing, and sharing tables
-- between demos makes it impossible to tell isolation from coincidence.
-- Nothing below joins to wh.orders or wh.promotions.

CREATE OR REPLACE TABLE wh.queues AS
SELECT * FROM (VALUES
    (1, 'billing',   4,  'high'),
    (2, 'technical', 8,  'medium'),
    (3, 'onboarding',24, 'low'),
    (4, 'security',  2,  'critical')
) AS q(queue_id, queue_name, sla_hours, severity_band);

CREATE OR REPLACE TABLE wh.agents AS
SELECT
    i                                                    AS agent_id,
    'Agent ' || lpad(i::VARCHAR, 3, '0')                 AS agent_name,
    ['frontline','escalations','platform'][1 + (i % 3)]  AS team,
    ['junior','senior','principal'][1 + (i % 3)]         AS seniority
FROM range(1, 121) t(i);

CREATE OR REPLACE TABLE wh.tickets AS
SELECT
    500000 + i                                              AS ticket_id,
    1 + (i % 120)                                           AS agent_id,
    1 + (i % 4)                                             AS queue_id,
    ['P1','P2','P3','P4'][1 + (i % 4)]                      AS priority,
    TIMESTAMP '2026-09-02 00:00:00' + INTERVAL (i % 21) DAY
                                    + INTERVAL (i % 17) HOUR AS opened_at,
    ['email','chat','phone'][1 + (i % 3)]                   AS channel,
    5 + ((i * 13) % 400)                                    AS first_response_mins
FROM range(1, 3001) t(i);

-- One ticket deliberately carries the same numeric id as the hero *order*.
-- Two systems reusing an id space is ordinary, and it is the sharpest test of
-- whether a trace keyed on 88231 can pull rows from the wrong DAG.
INSERT INTO wh.tickets VALUES
    (88231, 42, 4, 'P1', TIMESTAMP '2026-09-14 09:00:00', 'phone', 12);

-- One event per ticket, except for six that were reassigned and so have two.
-- A reassignment is a legitimate second row: joining to it fans the record
-- out without anything being wrong with either table.
CREATE OR REPLACE TABLE wh.ticket_events AS
SELECT
    600000 + i                                              AS event_id,
    500000 + i                                              AS ticket_id,
    ['resolved','closed','answered'][1 + (i % 3)]           AS event_type,
    DATE '2026-09-04' + INTERVAL (i % 18) DAY               AS occurred_on
FROM range(1, 3001) t(i);

INSERT INTO wh.ticket_events VALUES
    (690001, 500004, 'reassigned', DATE '2026-09-07'),
    (690002, 500011, 'reassigned', DATE '2026-09-08'),
    (690003, 500029, 'reassigned', DATE '2026-09-09'),
    (690004, 500040, 'reassigned', DATE '2026-09-10'),
    (690005, 500057, 'reassigned', DATE '2026-09-11'),
    (690006, 88231,  'reassigned', DATE '2026-09-15'),
    (690007, 88231,  'answered',   DATE '2026-09-15');
