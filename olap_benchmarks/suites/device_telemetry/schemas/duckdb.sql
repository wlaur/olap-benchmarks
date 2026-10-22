CREATE TABLE telemetry_device (
    device_id INTEGER NOT NULL,
    region VARCHAR NOT NULL,
    tenant INTEGER NOT NULL,
    model VARCHAR NOT NULL,
    rack VARCHAR,
    cadence_slots INTEGER NOT NULL,
    rated_power DOUBLE PRECISION NOT NULL
);

CREATE TABLE telemetry_reading (
    time TIMESTAMP NOT NULL,
    device_id INTEGER NOT NULL,
    sample_no BIGINT NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    region VARCHAR NOT NULL,
    tenant INTEGER NOT NULL,
    model VARCHAR NOT NULL,
    rack VARCHAR,
    cpu_user DOUBLE PRECISION,
    cpu_system DOUBLE PRECISION,
    memory_used DOUBLE PRECISION,
    disk_used DOUBLE PRECISION,
    net_rx DOUBLE PRECISION,
    net_tx DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    battery DOUBLE PRECISION,
    voltage DOUBLE PRECISION,
    current DOUBLE PRECISION,
    signal DOUBLE PRECISION,
    errors DOUBLE PRECISION
);
