-- adapted from settings for clickbench, not sure if these are optimal for this workload
ALTER DATABASE postgres
SET
    timescaledb.enable_chunk_skipping to ON;

ALTER DATABASE postgres
SET
    work_mem TO '1GB';

CREATE TABLE data_small_eav (
    time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    id INTEGER NOT NULL,
    value REAL
);

CREATE TABLE data_large_eav (
    time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    id INTEGER NOT NULL,
    value REAL
);

SELECT
    create_hypertable(
        'data_small_eav',
        'time',
        chunk_time_interval => INTERVAL '1 days'
    );

SELECT
    create_hypertable(
        'data_large_eav',
        'time',
        chunk_time_interval => INTERVAL '7 days'
    );

ALTER TABLE
    data_small_eav
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.orderby = 'time',
        timescaledb.segmentby = 'id'
    );

ALTER TABLE
    data_large_eav
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.orderby = 'time',
        timescaledb.segmentby = 'id'
    );
