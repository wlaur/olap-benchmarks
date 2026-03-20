SELECT
    create_hypertable(
        'data_wide',
        'time',
        chunk_time_interval => INTERVAL '1 days',
        migrate_data => true
    );

ALTER TABLE
    data_wide
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.orderby = 'time'
    );
