SELECT
    create_hypertable(
        'data_small_wide',
        'time',
        chunk_time_interval => INTERVAL '1 days'
    );

ALTER TABLE
    data_small_wide
SET
    (
        timescaledb.enable_columnstore = true,
        timescaledb.orderby = 'time'
    );


-- TODO: data_large_wide cannot be compressed (too wide, 18k elements per row vs 8k max)
-- this is not the same limit as the 1_600 column limit for postgres, timescaledb is even more limited
-- SELECT
--     create_hypertable(
--         'data_large_wide',
--         'time',
--         chunk_time_interval => INTERVAL '7 days'
--     );

-- ALTER TABLE
--     data_large_wide
-- SET
--     (
--         timescaledb.enable_columnstore = true,
--         timescaledb.orderby = 'time'
--     );
