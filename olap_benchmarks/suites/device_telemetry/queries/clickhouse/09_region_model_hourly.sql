SELECT toStartOfHour(time) AS bucket, model,
       COUNT(*) AS samples, avgOrNull(cpu_user) AS avg_cpu, maxOrNull(memory_used) AS max_memory
FROM telemetry_reading
WHERE region = 'region_1' AND time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
GROUP BY bucket, model
ORDER BY bucket, model
