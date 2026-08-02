ALTER DATABASE postgres SET work_mem TO '50MB';

CREATE TABLE chat_thread (
    thread_id     char(36)  NOT NULL,
    user_id       char(36)  NOT NULL,
    created_at    timestamp NOT NULL,
    updated_at    timestamp NOT NULL,
    title         text      NOT NULL,
    message_count integer   NOT NULL,
    settings      jsonb     NOT NULL
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

CREATE INDEX idx_chat_message_user_thread ON chat_message (user_id, thread_id, seq);
CREATE INDEX idx_chat_thread_user ON chat_thread (user_id, updated_at DESC);
