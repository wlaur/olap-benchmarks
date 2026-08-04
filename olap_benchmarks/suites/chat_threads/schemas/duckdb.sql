-- no primary keys: DuckDB crashes on constrained persistent tables past ~15k rows
-- (see the DuckDB upsert workaround in dbs/duckdb/__init__.py)
CREATE TABLE chat_thread (
    thread_id     VARCHAR   NOT NULL,
    user_id       VARCHAR   NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    title         VARCHAR   NOT NULL,
    message_count INTEGER   NOT NULL,
    settings      JSON      NOT NULL
);

CREATE TABLE chat_message (
    thread_id  VARCHAR   NOT NULL,
    message_id VARCHAR   NOT NULL,
    user_id    VARCHAR   NOT NULL,
    seq        INTEGER   NOT NULL,
    parent_id  VARCHAR,
    created_at TIMESTAMP NOT NULL,
    content    JSON      NOT NULL
);
