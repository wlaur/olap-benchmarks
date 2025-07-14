
CREATE TABLE customers
(
    customer_id integer not null,
    name text,
    birthday date,
    email text,
    address text,
    city text,
    zip text,
    state text,
    country text,
    PRIMARY KEY (customer_id)
);

CREATE TABLE products
(
    product_id integer not null,
    name text,
    description text,
    category text,
    price decimal(10,2),
    stock int,
    PRIMARY KEY (product_id)
);

CREATE TABLE orders
(
    order_id serial not null,
    customer_id integer not null,
    created_at timestamptz not null,
    PRIMARY KEY (order_id)
);

CREATE TABLE order_items
(
    order_id integer not null,
    product_id integer not null,
    amount integer not null,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE order_events
(
    order_id integer not null,
    counter integer,
    event_created timestamptz not null,
    event_type text not null,
    satisfaction real not null,
    processor text not null,
    backup_processor text,
    event_payload jsonb
);

SELECT create_hypertable('order_events', 'event_created', chunk_time_interval => interval '3 day', create_default_indexes => false);

SELECT * FROM enable_chunk_skipping('order_events', 'order_id');

ALTER TABLE order_events SET (timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'order_id, event_created');
