CREATE SEQUENCE IF NOT EXISTS seq_benchmark START 1;

CREATE TABLE IF NOT EXISTS benchmark (
    id INTEGER PRIMARY KEY DEFAULT nextval('seq_benchmark'),
    name TEXT NOT NULL,
    operation TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    deleted_at TIMESTAMP,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS metric (
    benchmark_id INTEGER REFERENCES benchmark(id),
    ts TIMESTAMP NOT NULL,
    cpu_percent REAL NOT NULL,
    mem_mb INTEGER NOT NULL,
    disk_mb INTEGER NOT NULL
);
