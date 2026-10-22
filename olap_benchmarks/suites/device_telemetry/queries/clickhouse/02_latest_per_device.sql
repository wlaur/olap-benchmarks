SELECT device_id, tupleElement(point, 1) AS time,
       tupleElement(point, 2) AS cpu_user, tupleElement(point, 3) AS memory_used
FROM (
    SELECT device_id, argMax(tuple(time, cpu_user, memory_used), time) AS point
    FROM telemetry_reading
    GROUP BY device_id
) latest
ORDER BY device_id
