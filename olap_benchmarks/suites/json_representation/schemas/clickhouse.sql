CREATE TABLE json_raw (j String CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE json_prepared (
    kind Nullable(String) CODEC(ZSTD(1)),
    operation Nullable(String) CODEC(ZSTD(1)),
    collection Nullable(String) CODEC(ZSTD(1)),
    did Nullable(String) CODEC(ZSTD(1)),
    time_us_text Nullable(String) CODEC(ZSTD(1)),
    time_us Nullable(Int64) CODEC(ZSTD(1))
)
ENGINE = MergeTree ORDER BY tuple();
