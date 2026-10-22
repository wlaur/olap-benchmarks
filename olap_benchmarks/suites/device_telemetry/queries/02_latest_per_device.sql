SELECT device_id, time, cpu_user, memory_used
FROM (
    SELECT device_id, time, cpu_user, memory_used,
           ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY time DESC) AS position
    FROM telemetry_reading
) ranked
WHERE position = 1
ORDER BY device_id
