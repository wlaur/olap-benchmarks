SELECT
    create_hypertable(
        'data_tall',
        'time',
        chunk_time_interval => INTERVAL '7 days',
        migrate_data => true
    );

ALTER TABLE
    data_tall
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.orderby = 'time'
    );
