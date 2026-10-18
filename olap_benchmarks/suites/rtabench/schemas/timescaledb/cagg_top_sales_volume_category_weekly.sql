-- CAGG_TOP_SALES_VOLUME_CATEGORY_WEEKLY
CREATE MATERIALIZED VIEW cagg_top_sales_volume_category_weekly WITH (timescaledb.continuous) AS
SELECT
	time_bucket('1 week', event_created) AS event_created,
  p.category,
  sum(oi.amount * p.price) AS volume
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN order_events oe ON oe.order_id = oi.order_id
WHERE
    event_type = 'Delivered'
GROUP BY
    1,2
WITH NO DATA;

SELECT set_chunk_time_interval('cagg_top_sales_volume_category_weekly', INTERVAL '1 year');
CALL refresh_continuous_aggregate('cagg_top_sales_volume_category_weekly',NULL,NULL);
CREATE INDEX ON cagg_top_sales_volume_category_weekly(event_created,volume DESC);
