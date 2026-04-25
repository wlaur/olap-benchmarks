-- data_large uses an EAV layout. Chunks span 14 days so the long time range
-- (~7 years) yields a few hundred chunks rather than thousands.
SELECT
    create_hypertable(
        'data_large',
        'time',
        chunk_time_interval => INTERVAL '14 days'
    );

ALTER TABLE
    data_large
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.segmentby = 'metric_name',
        timescaledb.orderby = 'time'
    );
