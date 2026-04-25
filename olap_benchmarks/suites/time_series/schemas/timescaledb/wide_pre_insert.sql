-- data_wide uses an EAV layout (time, metric_name, value) so that PG's
-- 8160-byte tuple limit is no longer a barrier and TimescaleDB columnstore
-- compression can be enabled with one segment per metric.
SELECT
    create_hypertable(
        'data_wide',
        'time',
        chunk_time_interval => INTERVAL '1 days'
    );

ALTER TABLE
    data_wide
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.segmentby = 'metric_name',
        timescaledb.orderby = 'time'
    );
