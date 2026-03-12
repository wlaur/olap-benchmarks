-- data_large_wide is too wide for Timescale columnstore/compression
-- (18k elements per row vs 8k max), but plain hypertable chunking still helps with queries
-- convert after insert so timescaledb-parallel-copy can use all workers against a plain table
SELECT
    create_hypertable(
        'data_large_wide',
        'time',
        chunk_time_interval => INTERVAL '7 days',
        migrate_data => true
    );
