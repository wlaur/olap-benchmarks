SELECT time, cpu_user, memory_used, temperature, battery
FROM telemetry_reading
WHERE device_id = 42 AND time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
ORDER BY time
