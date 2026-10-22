CREATE TABLE json_raw (j VARCHAR NOT NULL);

CREATE TABLE json_prepared (
    kind VARCHAR,
    operation VARCHAR,
    collection VARCHAR,
    did VARCHAR,
    time_us_text VARCHAR,
    time_us BIGINT
);
