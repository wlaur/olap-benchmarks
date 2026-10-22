CREATE TABLE telemetry_device (
    device_id INTEGER NOT NULL,
    region VARCHAR NOT NULL,
    tenant INTEGER NOT NULL,
    model VARCHAR NOT NULL,
    rack VARCHAR,
    cadence_slots INTEGER NOT NULL,
    rated_power DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (device_id)
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
    errors DOUBLE PRECISION,
    PRIMARY KEY (device_id, time)
);

CREATE INDEX telemetry_time ON telemetry_reading (time);
CREATE INDEX telemetry_tenant_time ON telemetry_reading (tenant, time);
CREATE INDEX telemetry_region_time ON telemetry_reading (region, time);

SELECT create_hypertable('telemetry_reading', by_range('time', INTERVAL '1 day'));
ALTER TABLE telemetry_reading SET (
    timescaledb.enable_columnstore = true,
    timescaledb.segmentby = 'device_id',
    timescaledb.orderby = 'time DESC'
);
