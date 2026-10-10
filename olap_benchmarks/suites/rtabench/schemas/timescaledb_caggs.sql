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

-- CAGG_ORDER_EVENTS_Q12
CREATE MATERIALIZED VIEW cagg_order_events_q12 WITH (timescaledb.continuous) AS
SELECT time_bucket('1 week', event_created) as week,
       order_id,
       max(satisfaction) AS satisfaction
FROM order_events
GROUP BY week, order_id;

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

-- CAGG_TOP_SELLING_PRODUCT_SEMESTER
CREATE MATERIALIZED VIEW cagg_top_selling_product_semester WITH (timescaledb.continuous) AS
SELECT
	time_bucket('6 month', event_created) AS event_created,
    product_id,
    p.name AS product_name,
    sum(oi.amount * p.price) AS volume,
    event_payload->>'terminal' as terminal
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN order_events oe ON oe.order_id = oi.order_id
GROUP BY
    1, terminal, product_id, product_name
;

CREATE INDEX ON cagg_top_selling_product_semester(terminal,event_created,volume desc);

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
