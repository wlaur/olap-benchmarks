-- wide tables are created dynamically and configured after creation
-- indexes are also created on (id, time) for the eav tables
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
        chunk_time_interval => INTERVAL '7 days'
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
        timescaledb.compress,
        timescaledb.compress_segmentby = 'id'
    );

ALTER TABLE
    data_large_eav
SET
    (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'id'
    );
