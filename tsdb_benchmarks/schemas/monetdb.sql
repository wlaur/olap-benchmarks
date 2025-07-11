create table if not exists customers
(
    customer_id integer not null,
    name text,
    birthday timestamp,
    email text,
    address text,
    city text,
    zip text,
    state text,
    country text,
    primary key (customer_id)
);

create table if not exists products
(
    product_id integer not null,
    name text,
    description text,
    category text,
    price decimal(10,2),
    stock int,
    primary key (product_id)
);

create table if not exists orders
(
    order_id integer not null,
    customer_id integer not null,
    created_at timestamp not null,
    primary key (order_id)
);

create table if not exists order_items
(
    order_id integer not null,
    product_id integer not null,
    amount integer not null,
    primary key (order_id, product_id)
);

create table if not exists order_events
(
    order_id integer not null,
    counter integer,
    event_created timestamp not null,
    event_type varchar(100) not null,
    satisfaction real not null,
    processor text not null,
    backup_processor text,
    event_payload json
);
