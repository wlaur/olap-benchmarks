SELECT
    create_hypertable(
        'data_small_wide',
        'time',
        chunk_time_interval => INTERVAL '7 days'
    );

ALTER TABLE
    data_small_wide
SET
    (
        timescaledb.compress,
        timescaledb.compress_orderby = 'time'
    );

SELECT
    create_hypertable(
        'data_large_wide',
        'time',
        chunk_time_interval => INTERVAL '7 days'
    );

ALTER TABLE
    data_large_wide
SET
    (
        timescaledb.compress,
        timescaledb.compress_orderby = 'time'
    );
