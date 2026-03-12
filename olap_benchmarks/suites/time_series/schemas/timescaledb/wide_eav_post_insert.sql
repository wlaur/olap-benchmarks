SELECT
    create_hypertable(
        'data_wide_eav',
        'time',
        chunk_time_interval => INTERVAL '1 days',
        migrate_data => true
    );

CREATE INDEX data_wide_eav_id_time_index ON data_wide_eav (id, time);

ALTER TABLE
    data_wide_eav
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.segmentby = 'id',
        timescaledb.orderby = 'time'
    );
