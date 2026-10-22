SELECT device_id, time, cpu_user, memory_used
FROM telemetry_reading
WHERE device_id = 42
ORDER BY time DESC
LIMIT 1
