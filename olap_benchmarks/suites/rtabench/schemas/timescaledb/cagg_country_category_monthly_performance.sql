-- CAGG_COUNTRY_CATEGORY_MONTHLY_PERFORMANCE
CREATE MATERIALIZED VIEW cagg_country_category_monthly_performance WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', oe.event_created) as event_created,
	c.country,
    p.category as product_category,
    sum(p.price * oi.amount) as revenue
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN orders o ON o.order_id = oi.order_id
    INNER JOIN customers c ON c.customer_id = o.customer_id
    INNER JOIN order_events oe ON oe.order_id = o.order_id
where oe.event_type = 'Delivered'
GROUP BY
    1,2,3
WITH NO DATA;

select set_chunk_time_interval('cagg_country_category_monthly_performance', INTERVAL '3 month');
ALTER MATERIALIZED VIEW cagg_country_category_monthly_performance set (timescaledb.compress = true);
CALL refresh_continuous_aggregate('cagg_country_category_monthly_performance', '2024-01-01', '2025-01-01');
SELECT compress_chunk(c, true) FROM show_chunks('cagg_country_category_monthly_performance') c;
