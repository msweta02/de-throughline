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
