-- no primary keys: a declared PK triggers the constrained-append regression recorded in
-- MONETDB.md, and time_series omits them for the same reason. This is MonetDB's honest
-- default for a bulk-loaded table, so point lookups scan the id column.
CREATE TABLE chat_thread (
    thread_id     varchar(36)  NOT NULL,
    user_id       varchar(36)  NOT NULL,
    created_at    timestamp    NOT NULL,
    updated_at    timestamp    NOT NULL,
    title         varchar(256) NOT NULL,
    message_count integer      NOT NULL,
    settings      json         NOT NULL
);

CREATE TABLE chat_message (
    thread_id  varchar(36) NOT NULL,
    message_id varchar(36) NOT NULL,
    user_id    varchar(36) NOT NULL,
    seq        integer     NOT NULL,
    parent_id  varchar(36),
    created_at timestamp   NOT NULL,
    content    json        NOT NULL
);
