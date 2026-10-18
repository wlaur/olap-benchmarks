-- Continuous aggregates read by the 1000-series queries, ported from upstream RTABench
-- timescaledb/caggs.sql. They are created after order_events is loaded and compressed, as
-- upstream does, and each statement runs outside a transaction because creating and refreshing
-- a continuous aggregate cannot run inside one. Upstream passes 'UTC' to two time_bucket()
-- calls; that overload only exists for timestamptz and event_created is a timestamp here.

-- CAGG_ORDER_EVENTS_0004
CREATE MATERIALIZED VIEW cagg_order_events_0004 WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 day', event_created) AS day,
  count(*) AS count
FROM order_events
WHERE
  event_payload -> 'status' @> '["Delayed", "Priority"]'
GROUP BY day
order by count(*) desc
WITH NO DATA;

select set_chunk_time_interval('cagg_order_events_0004', INTERVAL '30 days');
CALL refresh_continuous_aggregate('cagg_order_events_0004', '2024-01-01', '2025-01-01');
