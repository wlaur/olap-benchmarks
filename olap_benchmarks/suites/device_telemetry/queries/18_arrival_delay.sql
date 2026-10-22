SELECT region, COUNT(*) AS samples,
       SUM(CASE WHEN ingested_at >= '2025-01-15 00:00:00' THEN 1 ELSE 0 END) AS arrived_after_cutoff,
       MAX(ingested_at) AS latest_arrival
FROM telemetry_reading
WHERE time >= '2025-01-14 23:00:00' AND time < '2025-01-15 00:00:00'
GROUP BY region
ORDER BY region
