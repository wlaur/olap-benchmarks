ALTER DATABASE postgres SET work_mem TO '50MB';

-- chat_thread stays a plain rowstore table: it is small, and it is the table the
-- mutate workload updates in place, which a hypertable would only make slower.
CREATE TABLE chat_thread (
    thread_id     char(36)  NOT NULL,
    user_id       char(36)  NOT NULL,
    created_at    timestamp NOT NULL,
    updated_at    timestamp NOT NULL,
    title         text      NOT NULL,
    message_count integer   NOT NULL,
    settings      jsonb     NOT NULL,
    PRIMARY KEY (thread_id)
);

CREATE TABLE chat_message (
    thread_id  char(36)  NOT NULL,
    message_id char(36)  NOT NULL,
    user_id    char(36)  NOT NULL,
    seq        integer   NOT NULL,
    parent_id  char(36),
    created_at timestamp NOT NULL,
    content    jsonb     NOT NULL
);

-- 30-day chunks put ~11k messages per chunk at SF1 and ~110k at SF10, so a thread
-- (which spans hours to a few days) almost always lands in one or two chunks.
SELECT create_hypertable('chat_message', by_range('created_at', INTERVAL '30 days'));

-- No compress_segmentby: user_id looks like the natural segment key, but a user holds
-- only ~54 messages spread across ~24 chunks, so segments would average two rows and
-- compress worse than not compressing at all. Ordering instead gives full 1000-row
-- batches and still yields per-batch min/max pruning on the serving-path predicates.
ALTER TABLE chat_message SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'user_id, thread_id, seq'
);

CREATE INDEX idx_chat_message_user_thread ON chat_message (user_id, thread_id, seq);
CREATE INDEX idx_chat_message_created ON chat_message (created_at);

-- No unique constraint on chat_message: a hypertable's unique index must include the
-- partitioning column, so ON CONFLICT (message_id) is not expressible. stream_update_message
-- therefore fails on TimescaleDB by construction, which is itself a result worth recording.
CREATE INDEX idx_chat_thread_user ON chat_thread (user_id, updated_at DESC);
