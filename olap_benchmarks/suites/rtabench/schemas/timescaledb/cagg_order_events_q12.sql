-- CAGG_ORDER_EVENTS_Q12
CREATE MATERIALIZED VIEW cagg_order_events_q12 WITH (timescaledb.continuous) AS
SELECT time_bucket('1 week', event_created) as week,
       order_id,
       max(satisfaction) AS satisfaction
FROM order_events
GROUP BY week, order_id;
