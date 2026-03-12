-- columnstore/compression is not enabled: data_large has 18k elements per row which exceeds
-- PostgreSQL's 8160-byte max tuple size for compressed rows (confirmed TimescaleDB 2.25.0 / PG 18)
SELECT
    create_hypertable(
        'data_large',
        'time',
        chunk_time_interval => INTERVAL '14 days',
        migrate_data => true
    );
