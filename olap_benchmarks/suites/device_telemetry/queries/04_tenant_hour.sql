SELECT COUNT(*) AS samples, COUNT(cpu_user) AS valid_samples,
       AVG(cpu_user) AS avg_cpu, MIN(cpu_user) AS min_cpu, MAX(cpu_user) AS max_cpu
FROM telemetry_reading
WHERE tenant = 7 AND time >= '2025-01-14 23:00:00' AND time < '2025-01-15 00:00:00'
