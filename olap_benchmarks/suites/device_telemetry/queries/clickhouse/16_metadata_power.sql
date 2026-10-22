SELECT d.region, d.rack, COUNT(*) AS samples,
       avgOrNull(r.cpu_user * d.rated_power / 100.0) AS avg_estimated_power
FROM telemetry_reading r
JOIN telemetry_device d ON r.device_id = d.device_id
WHERE r.time >= '2025-01-14 00:00:00' AND r.time < '2025-01-15 00:00:00'
GROUP BY d.region, d.rack
ORDER BY d.region, d.rack NULLS FIRST
