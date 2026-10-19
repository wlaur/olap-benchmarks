-- CAGG_ORDER_EVENTS_Q13
CREATE MATERIALIZED VIEW cagg_order_events_q13 WITH (timescaledb.continuous) AS
SELECT time_bucket('1 month', event_created) as month,
       order_id,
       count(*) FILTER (WHERE backup_processor <> '') as count_with_backup,
       count(*) FILTER (WHERE backup_processor is null) as count_without_backup,
       avg(satisfaction) FILTER (WHERE backup_processor <> '') as avg_satisfaction_with_backup,
       avg(satisfaction) FILTER (WHERE backup_processor is null) as avg_satisfaction_without_backup
FROM order_events
GROUP BY month, order_id;
