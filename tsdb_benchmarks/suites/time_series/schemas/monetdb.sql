create table if not exists process_small_eav (
    time timestamp primary key,
    id smallint primary key,
    value float32
);

create table if not exists process_medium_eav (
    time timestamp primary key,
    id smallint primary key,
    value float32
);

create table if not exists process_large_eav (
    time timestamp primary key,
    id smallint primary key,
    value float32
);

create table if not exists process_huge_eav (
    time timestamp primary key,
    id smallint primary key,
    value float32
);

-- process_..._wide is created dynamically when inserted
