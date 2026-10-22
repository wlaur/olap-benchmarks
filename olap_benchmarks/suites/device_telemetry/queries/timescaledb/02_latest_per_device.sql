SELECT DISTINCT ON (device_id) device_id, time, cpu_user, memory_used
FROM telemetry_reading
ORDER BY device_id, time DESC
