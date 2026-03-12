-- convert after insert so timescaledb-parallel-copy can use all workers against a plain table
SELECT
    create_hypertable(
        'data_large_wide',
        'time',
        chunk_time_interval => INTERVAL '7 days',
        migrate_data => true
    );

ALTER TABLE
    data_large_wide
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.orderby = 'time'
    );
