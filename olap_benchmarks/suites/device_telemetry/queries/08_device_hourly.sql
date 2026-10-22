SELECT DATE_TRUNC('hour', time) AS bucket, COUNT(*) AS samples,
       COUNT(cpu_user) AS valid_samples, AVG(cpu_user) AS avg_cpu, AVG(memory_used) AS avg_memory
FROM telemetry_reading
WHERE device_id = 42 AND time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
GROUP BY bucket
ORDER BY bucket
