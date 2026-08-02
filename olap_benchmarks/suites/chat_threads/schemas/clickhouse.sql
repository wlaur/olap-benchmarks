CREATE TABLE chat_thread
(
    thread_id     String,
    user_id       String,
    created_at    DateTime64(3),
    updated_at    DateTime64(3),
    title         String,
    message_count Int32,
    settings      JSON(
        topic LowCardinality(String),
        model_preference LowCardinality(String),
        pinned Bool,
        archived Bool) CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (user_id, thread_id);

CREATE TABLE chat_message
(
    thread_id  String,
    message_id String,
    user_id    String,
    seq        Int32,
    parent_id  Nullable(String),
    created_at DateTime64(3),
    content    JSON(
        role LowCardinality(String),
        model LowCardinality(String),
        stop_reason LowCardinality(String),
        latency_ms UInt32,
        usage.input_tokens UInt32,
        usage.output_tokens UInt32,
        usage.cache_read_tokens UInt32) CODEC(ZSTD(1))
)
ENGINE = MergeTree
ORDER BY (user_id, thread_id, seq);
