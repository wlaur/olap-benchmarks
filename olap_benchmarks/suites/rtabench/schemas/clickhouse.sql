DROP TABLE IF EXISTS customers;

DROP TABLE IF EXISTS products;

DROP TABLE IF EXISTS orders;

DROP TABLE IF EXISTS order_items;

DROP TABLE IF EXISTS order_events;

CREATE TABLE customers (
    customer_id integer not null,
    name text,
    birthday DateTime64,
    email text,
    address text,
    city text,
    zip text,
    state text,
    country text,
    PRIMARY KEY (customer_id)
);

CREATE TABLE products (
    product_id integer not null,
    name text,
    description text,
    category text,
    price decimal(10, 2),
    stock int,
    PRIMARY KEY (product_id)
);

CREATE TABLE orders (
    order_id integer not null,
    customer_id integer not null,
    created_at timestamp not null,
    PRIMARY KEY (order_id)
);

CREATE TABLE order_items (
    order_id integer not null,
    product_id integer not null,
    amount integer not null,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE order_events (
    order_id integer not null,
    counter integer,
    event_created timestamp not null,
    event_type text not null,
    satisfaction Float32 not null,
    processor text not null,
    -- ClickHouse's text is not nullable, and this is the only column in the suite holding NULLs
    backup_processor Nullable(String),
    event_payload text,
    PRIMARY KEY (order_id, event_created)
) ENGINE = MergeTree;

-- Incremental materialized views, ported from upstream RTABench clickhouse/mat_views.sql.
-- They are created here, before any INSERT, because a ClickHouse materialized view only sees
-- rows written after it exists. The 1000-series queries read these tables.

DROP TABLE IF EXISTS order_events_q4;
DROP TABLE IF EXISTS order_events_q8;
DROP TABLE IF EXISTS order_events_q12;
DROP TABLE IF EXISTS order_events_q13;
DROP TABLE IF EXISTS order_events_per_terminal_per_hour;
DROP TABLE IF EXISTS country_category_monthly_performance;
DROP TABLE IF EXISTS top_selling_month_product_q17;
DROP TABLE IF EXISTS top_sales_volume_product;
DROP TABLE IF EXISTS customers_with_most_orders_delivered;

DROP VIEW IF EXISTS mv_order_events_q4;
DROP VIEW IF EXISTS mv_order_events_q8;
DROP VIEW IF EXISTS mv_order_events_q12;
DROP VIEW IF EXISTS mv_order_events_q13;
DROP VIEW IF EXISTS mv_order_events_per_terminal_per_hour;
DROP VIEW IF EXISTS mv_country_category_monthly_performance;
DROP VIEW IF EXISTS mv_top_selling_month_product_q17;
DROP VIEW IF EXISTS mv_top_sales_volume_product;
DROP VIEW IF EXISTS mv_customers_with_most_orders_delivered;

CREATE TABLE order_events_q4
(
    day timestamp,
    delayed_orders integer
)
ENGINE = MergeTree
ORDER BY day;

CREATE MATERIALIZED VIEW mv_order_events_q4 TO order_events_q4 AS
SELECT
  toStartOfDay(event_created) AS day,
  count() AS delayed_orders
FROM order_events
WHERE
    hasAll(JSONExtract(event_payload, 'status', 'Array(Nullable(TEXT))'), ['Delayed', 'Priority'])
GROUP BY day
;

CREATE TABLE order_events_q8
(
    day timestamp,
    order_id integer,
    delayed_orders integer
)
ENGINE = MergeTree
ORDER BY day;

CREATE MATERIALIZED VIEW mv_order_events_q8 TO order_events_q8 AS
SELECT
  toStartOfDay(event_created) AS day,
  order_id,
  count() AS delayed_orders
FROM order_events
WHERE
    hasAll(JSONExtract(event_payload, 'status', 'Array(Nullable(TEXT))'), ['Delayed', 'Priority'])
GROUP BY day, order_id
;

CREATE TABLE order_events_q12
(
    week timestamp,
    order_id integer,
    satisfaction Float32
)
ENGINE = MergeTree
ORDER BY (week, order_id);

CREATE MATERIALIZED VIEW mv_order_events_q12 TO order_events_q12 AS
SELECT toStartOfWeek(event_created) as week,
       order_id,
       maxSimpleState(satisfaction) AS satisfaction
FROM order_events
GROUP BY week, order_id
;

CREATE TABLE order_events_q13
(
    month timestamp,
    order_id integer,
    count_with_backup integer,
    count_without_backup integer,
    avg_satisfaction_with_backup Float32,
    avg_satisfaction_without_backup Float32
)
ENGINE = MergeTree
ORDER BY (month, order_id);

CREATE MATERIALIZED VIEW mv_order_events_q13 TO order_events_q13 AS
SELECT toStartOfMonth(event_created) as month,
       order_id,
       count() FILTER (WHERE backup_processor is not null) as count_with_backup,
       count() FILTER (WHERE backup_processor is null) as count_without_backup,
       avg(satisfaction) FILTER (WHERE backup_processor is not null) as avg_satisfaction_with_backup,
       avg(satisfaction) FILTER (WHERE backup_processor is null) as avg_satisfaction_without_backup
FROM order_events
GROUP BY month, order_id
;

CREATE TABLE order_events_per_terminal_per_hour
(
    hour timestamp,
    terminal text,
    event_count integer,
    unique_orders integer
)
ENGINE = MergeTree
ORDER BY (hour, terminal);

CREATE MATERIALIZED VIEW mv_order_events_per_terminal_per_hour TO order_events_per_terminal_per_hour AS
SELECT
    toStartOfHour(event_created) as hour,
    JSONExtractString(event_payload, 'terminal') as terminal,
    count(*) as event_count,
    count(DISTINCT JSONExtractString(event_payload, 'order_id')) as unique_orders
FROM order_events
WHERE
    event_type IN ('Created', 'Departed', 'Delivered')
GROUP BY hour, terminal
;

CREATE TABLE country_category_monthly_performance
(
    event_created timestamp,
    country text,
    product_category text,
    revenue Float32
)
ENGINE = MergeTree
ORDER BY (event_created, country, product_category);

CREATE MATERIALIZED VIEW mv_country_category_monthly_performance TO country_category_monthly_performance AS
SELECT
    toStartOfMonth(oe.event_created) as event_created,
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
    event_created,
    country,
    product_category;

CREATE TABLE top_selling_month_product_q17
(
    event_created timestamp,
    product_id integer,
    product_name text,
    amount integer
)
ENGINE = MergeTree
ORDER BY (event_created, product_id, product_name);

CREATE MATERIALIZED VIEW mv_top_selling_month_product_q17 TO top_selling_month_product_q17 AS
SELECT
    toStartOfMonth(event_created) AS event_created,
    p.product_id as product_id,
    p.name as product_name,
    sum(amount) as amount
FROM
    products p
    INNER JOIN order_items oi USING(product_id)
    INNER JOIN order_events oe USING(order_id)
WHERE
    event_type = 'Delivered'
GROUP BY
    event_created,
    product_id,
    product_name;

CREATE TABLE top_sales_volume_product
(
    event_created timestamp,
    product_id integer,
    product_name text,
    volume Decimal(10,2),
    category text,
    terminal text
)
ENGINE = MergeTree
ORDER BY (event_created, product_id, product_name, terminal);

CREATE MATERIALIZED VIEW mv_top_sales_volume_product TO top_sales_volume_product AS
SELECT
    toStartOfMonth(event_created) AS event_created,
    product_id,
    p.name as product_name,
    sum(oi.amount * p.price) AS volume,
    p.category,
    JSONExtractString(event_payload, 'terminal') as terminal
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN order_events oe ON oe.order_id = oi.order_id
WHERE
    event_type = 'Delivered'
GROUP BY
    event_created,
    product_id,
    product_name,
    category,
    terminal;

CREATE TABLE customers_with_most_orders_delivered
(
    event_created timestamp,
    customer_id integer,
    customer_name text,
    customer_orders integer
)
ENGINE = MergeTree
ORDER BY (event_created, customer_id, customer_name);

CREATE MATERIALIZED VIEW mv_customers_with_most_orders_delivered TO customers_with_most_orders_delivered AS
SELECT
    toStartOfMonth(oe.event_created) AS event_created,
    c.customer_id as customer_id,
    c.name as customer_name,
    count(o.order_id) as customer_orders
FROM
    customers c
    INNER JOIN orders o USING (customer_id)
    INNER JOIN order_events oe USING (order_id)
WHERE
    oe.event_type = 'Delivered'
GROUP BY
    event_created, customer_id, customer_name
;
