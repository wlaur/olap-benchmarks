SELECT time_bucket(INTERVAL '1 hour', time) AS bucket, model,
       COUNT(*) AS samples, AVG(cpu_user) AS avg_cpu, MAX(memory_used) AS max_memory
FROM telemetry_reading
WHERE region = 'region_1' AND time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
GROUP BY bucket, model
ORDER BY bucket, model
