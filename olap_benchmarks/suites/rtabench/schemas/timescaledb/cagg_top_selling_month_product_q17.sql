-- CAGG_TOP_SELLING_MONTH_PRODUCT_Q17
CREATE MATERIALIZED VIEW cagg_top_selling_month_product_q17 WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 month',event_created) AS event_created,
    p.product_id,
    p.name as product_name,
    sum(amount) as amount
FROM
    products p
    INNER JOIN order_items oi USING(product_id)
    INNER JOIN order_events oe USING(order_id)
WHERE
    event_type = 'Delivered'
GROUP BY
    1,2,3
WITH NO DATA;

select set_chunk_time_interval('cagg_top_selling_month_product_q17', INTERVAL '3 month');
ALTER MATERIALIZED VIEW cagg_top_selling_month_product_q17 set (timescaledb.compress = true);
CALL refresh_continuous_aggregate('cagg_top_selling_month_product_q17', '2024-01-01', '2025-01-01');
SELECT compress_chunk(c, true) FROM show_chunks('cagg_top_selling_month_product_q17') c;
