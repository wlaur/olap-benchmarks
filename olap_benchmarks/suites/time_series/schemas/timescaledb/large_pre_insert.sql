-- data_large uses an EAV layout with 30-day chunks. Total span ≈ 7.6 years
-- → ~92 chunks. 30 days keeps the per-chunk row count (~65 M EAV rows) low
-- enough that columnstore sort fits in work_mem (4 GB) and the chunk
-- compresses in roughly half the time of a 60-day chunk. With 60-day chunks
-- the compress backlog grew during data_large ingest because each chunk
-- took ~3 min while inserts produced one chunk every 1.5 min or so.
SELECT
    create_hypertable(
        'data_large',
        by_range('time', INTERVAL '30 days')
    );

ALTER TABLE
    data_large
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.segmentby = 'metric_name',
        timescaledb.orderby = 'time DESC',
        autovacuum_enabled = false,
        toast.autovacuum_enabled = false
    );
