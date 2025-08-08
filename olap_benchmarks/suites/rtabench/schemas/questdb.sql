CREATE TABLE customers (
    customer_id integer not null,
    name varchar,
    birthday date,
    email varchar,
    address varchar,
    city varchar,
    zip varchar,
    state varchar,
    country varchar
);

CREATE TABLE products (
    product_id integer not null,
    name varchar,
    description varchar,
    category varchar,
    price double,
    stock integer
);

CREATE TABLE orders (
    order_id integer not null,
    customer_id integer not null,
    created_at timestamp not null
);

CREATE TABLE order_items (
    order_id integer not null,
    product_id integer not null,
    amount integer not null
);

CREATE TABLE order_events (
    order_id integer not null,
    counter integer,
    event_created timestamp not null,
    event_type varchar not null,
    satisfaction double not null,
    processor varchar not null,
    backup_processor varchar,
    event_payload varchar
);
