SELECT g.device_id, g.time, g.sample_no - g.previous_no AS gap_slots
FROM (
    SELECT device_id, time, sample_no,
           LAG(sample_no) OVER (PARTITION BY device_id ORDER BY time) AS previous_no
    FROM telemetry_reading
    WHERE time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
) g
JOIN telemetry_device d ON g.device_id = d.device_id
WHERE g.sample_no - g.previous_no > d.cadence_slots
ORDER BY g.device_id, g.time
