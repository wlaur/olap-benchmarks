CREATE TABLE IF NOT EXISTS benchmark (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS metric (
    benchmark_id INTEGER REFERENCES benchmark(id),
    ts TIMESTAMP NOT NULL,
    cpu_percent REAL NOT NULL,
    mem_mb INTEGER NOT NULL,
    disk_mb INTEGER NOT NULL
);
