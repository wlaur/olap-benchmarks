-- data_large uses an EAV layout with 30-day chunks. Total span ≈ 7.6 years
-- → ~92 chunks. 30 days keeps the per-chunk row count (~65 M EAV rows) low
-- enough that compress_chunk()'s sort fits in work_mem (4 GB) and the chunk
-- compresses in ~half the time of a 60-day chunk. With 60-day chunks the
-- compress backlog grew during data_large ingest -- each chunk took roughly
-- 3 min to compress while inserts produced one chunk every 1.5 min or so.
SELECT
    create_hypertable(
        'data_large',
        'time',
        chunk_time_interval => INTERVAL '30 days'
    );

ALTER TABLE
    data_large
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.segmentby = 'metric_name',
        timescaledb.orderby = 'time',
        autovacuum_enabled = false,
        toast.autovacuum_enabled = false
    );
