SELECT region, COUNT(*) AS samples, COUNT(rack) AS tagged_samples,
       COUNT(cpu_user) AS cpu_samples, COUNT(battery) AS battery_samples,
       SUM(CASE WHEN cpu_user IS NULL AND battery IS NULL THEN 1 ELSE 0 END) AS both_missing
FROM telemetry_reading
WHERE time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
GROUP BY region
ORDER BY region
