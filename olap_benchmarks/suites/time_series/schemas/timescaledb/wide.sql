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


-- data_large_wide is too wide for Timescale columnstore/compression
-- (18k elements per row vs 8k max), but plain hypertable chunking may still help.
SELECT
    create_hypertable(
        'data_large_wide',
        'time',
        chunk_time_interval => INTERVAL '7 days'
    );
