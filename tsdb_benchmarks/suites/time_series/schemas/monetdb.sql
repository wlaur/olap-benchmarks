create table if not exists process_small_eav (
    time timestamp,
    id smallint,
    value real
    -- primary key (id, time)
);

create table if not exists process_medium_eav (
    time timestamp,
    id smallint,
    value real
    -- primary key (id, time)
);

-- create table if not exists process_large_eav (
--     time timestamp,
--     id smallint,
--     value real,
--     primary key (id, time)
-- );

-- create table if not exists process_huge_eav (
--     time timestamp,
--     id smallint,
--     value real,
--     primary key (id, time)
-- );

-- process_..._wide is created dynamically when inserted
