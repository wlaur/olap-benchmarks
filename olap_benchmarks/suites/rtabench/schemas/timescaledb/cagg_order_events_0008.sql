-- CAGG_ORDER_EVENTS_0008
CREATE MATERIALIZED VIEW cagg_order_events_0008 WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 week', event_created) AS week,
  order_id,
  count(*) AS count
FROM order_events
WHERE
  event_payload -> 'status' @> '["Delayed", "Priority"]'
GROUP BY week, order_id
;

CREATE INDEX ON cagg_order_events_0008(count desc, week);
