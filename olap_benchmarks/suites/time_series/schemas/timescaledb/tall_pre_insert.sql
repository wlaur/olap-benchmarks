-- data_tall stays wide (10 cols, fits within PG's tuple-size limit). The
-- hypertable is created on an empty table, then data is loaded.
SELECT
    create_hypertable(
        'data_tall',
        'time',
        chunk_time_interval => INTERVAL '7 days'
    );

ALTER TABLE
    data_tall
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.orderby = 'time'
    );
