CREATE TABLE telemetry_device (
    device_id Int32,
    region LowCardinality(String),
    tenant Int32,
    model LowCardinality(String),
    rack LowCardinality(Nullable(String)),
    cadence_slots Int32,
    rated_power Float64
)
ENGINE = MergeTree
ORDER BY device_id;

CREATE TABLE telemetry_reading (
    time DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    device_id Int32,
    sample_no Int64,
    ingested_at DateTime64(3, 'UTC'),
    region LowCardinality(String),
    tenant Int32,
    model LowCardinality(String),
    rack LowCardinality(Nullable(String)),
    cpu_user Nullable(Float64),
    cpu_system Nullable(Float64),
    memory_used Nullable(Float64),
    disk_used Nullable(Float64),
    net_rx Nullable(Float64),
    net_tx Nullable(Float64),
    temperature Nullable(Float64),
    battery Nullable(Float64),
    voltage Nullable(Float64),
    current Nullable(Float64),
    signal Nullable(Float64),
    errors Nullable(Float64)
)
ENGINE = MergeTree
PARTITION BY toDate(time)
ORDER BY (region, tenant, device_id, time)
SETTINGS old_parts_lifetime = 5;
