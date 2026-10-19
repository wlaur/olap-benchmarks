-- CAGG_ORDER_EVENTS_PER_TERMINAL_PER_HOUR
CREATE MATERIALIZED VIEW cagg_order_events_per_terminal_per_hour WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', event_created) as hour,
    event_payload->>'terminal' as terminal,
    count(*) as event_count,
    count(DISTINCT event_payload->>'order_id') as unique_orders
  FROM order_events
  WHERE
    event_type IN ('Created', 'Departed', 'Delivered')
  GROUP BY hour, terminal;

ALTER MATERIALIZED VIEW cagg_order_events_per_terminal_per_hour set (timescaledb.compress = true);
CALL refresh_continuous_aggregate('cagg_order_events_per_terminal_per_hour', '2024-01-01', '2025-01-01');
SELECT compress_chunk(c, true) FROM show_chunks('cagg_order_events_per_terminal_per_hour') c;
