SELECT COUNT(*) AS samples,
       AVG(cpu_user) AS avg_cpu_user,
       AVG(cpu_system) AS avg_cpu_system,
       AVG(memory_used) AS avg_memory_used,
       AVG(disk_used) AS avg_disk_used,
       AVG(net_rx) AS avg_net_rx,
       AVG(net_tx) AS avg_net_tx,
       AVG(temperature) AS avg_temperature,
       AVG(battery) AS avg_battery,
       AVG(voltage) AS avg_voltage,
       AVG(current) AS avg_current,
       AVG(signal) AS avg_signal,
       AVG(errors) AS avg_errors
FROM telemetry_reading
WHERE time >= '2025-01-14 23:00:00' AND time < '2025-01-15 00:00:00'
