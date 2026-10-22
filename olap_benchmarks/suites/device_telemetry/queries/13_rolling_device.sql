SELECT time, cpu_user,
       AVG(cpu_user) OVER (ORDER BY time ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_cpu
FROM telemetry_reading
WHERE device_id = 42 AND time >= '2025-01-14 00:00:00' AND time < '2025-01-15 00:00:00'
ORDER BY time
