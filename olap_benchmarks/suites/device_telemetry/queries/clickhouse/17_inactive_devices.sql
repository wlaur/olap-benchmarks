SELECT d.device_id, p.latest_time
FROM telemetry_device d
LEFT JOIN (
    SELECT device_id, maxOrNull(time) AS latest_time
    FROM telemetry_reading
    GROUP BY device_id
) p ON d.device_id = p.device_id
WHERE p.latest_time < '2025-01-14 00:00:00' OR p.latest_time IS NULL
ORDER BY d.device_id
