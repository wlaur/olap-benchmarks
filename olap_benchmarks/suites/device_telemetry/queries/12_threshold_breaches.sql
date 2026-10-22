SELECT region, COUNT(*) AS samples,
       SUM(CASE WHEN cpu_user >= 80 THEN 1 ELSE 0 END) AS cpu_breaches,
       SUM(CASE WHEN cpu_user >= 80 AND memory_used >= 60 THEN 1 ELSE 0 END) AS joint_breaches
FROM telemetry_reading
WHERE time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
GROUP BY region
ORDER BY region
