SELECT model, COUNT(*) AS samples, AVG(cpu_user) AS avg_cpu,
       AVG(memory_used) AS avg_memory, MAX(temperature) AS max_temperature
FROM telemetry_reading
WHERE region = 'region_1' AND time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
GROUP BY model
ORDER BY model
