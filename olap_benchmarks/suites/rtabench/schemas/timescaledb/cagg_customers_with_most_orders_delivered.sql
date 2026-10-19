-- CAGG_CUSTOMERS_WITH_MOST_ORDERS_DELIVERED
CREATE MATERIALIZED VIEW cagg_customers_with_most_orders_delivered WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month', oe.event_created) AS event_created,
    c.customer_id,
    c.name as customer_name,
    count(o.order_id) as customer_orders
FROM
    customers c
    INNER JOIN orders o USING (customer_id)
    INNER JOIN order_events oe USING (order_id)
WHERE
    oe.event_type = 'Delivered'
GROUP BY
    1,2,3
WITH NO DATA;

select set_chunk_time_interval('cagg_customers_with_most_orders_delivered', INTERVAL '6 month');
ALTER MATERIALIZED VIEW cagg_customers_with_most_orders_delivered set (timescaledb.compress = true);
CALL refresh_continuous_aggregate('cagg_customers_with_most_orders_delivered', '2024-01-01', '2025-01-01');
SELECT compress_chunk(c, true) FROM show_chunks('cagg_customers_with_most_orders_delivered') c;
