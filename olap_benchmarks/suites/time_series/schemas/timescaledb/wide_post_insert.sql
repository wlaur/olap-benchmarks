-- convert after insert so timescaledb-parallel-copy can use all workers against a plain table
-- columnstore/compression is not enabled: data_large_wide has 18k elements per row which exceeds
-- PostgreSQL's 8160-byte max tuple size for compressed rows (confirmed TimescaleDB 2.25.0 / PG 18)
SELECT
    create_hypertable(
        'data_large_wide',
        'time',
        chunk_time_interval => INTERVAL '7 days',
        migrate_data => true
    );
