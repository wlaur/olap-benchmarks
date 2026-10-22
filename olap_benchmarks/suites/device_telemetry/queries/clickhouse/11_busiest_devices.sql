SELECT device_id, COUNT(cpu_user) AS valid_samples, avgOrNull(cpu_user) AS avg_cpu
FROM telemetry_reading
WHERE time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
GROUP BY device_id
HAVING COUNT(cpu_user) >= 12
ORDER BY avg_cpu DESC, device_id
LIMIT 20
