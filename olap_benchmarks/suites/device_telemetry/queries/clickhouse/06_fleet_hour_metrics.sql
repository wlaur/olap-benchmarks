SELECT COUNT(*) AS samples,
       avgOrNull(cpu_user) AS avg_cpu_user,
       avgOrNull(cpu_system) AS avg_cpu_system,
       avgOrNull(memory_used) AS avg_memory_used,
       avgOrNull(disk_used) AS avg_disk_used,
       avgOrNull(net_rx) AS avg_net_rx,
       avgOrNull(net_tx) AS avg_net_tx,
       avgOrNull(temperature) AS avg_temperature,
       avgOrNull(battery) AS avg_battery,
       avgOrNull(voltage) AS avg_voltage,
       avgOrNull(current) AS avg_current,
       avgOrNull(signal) AS avg_signal,
       avgOrNull(errors) AS avg_errors
FROM telemetry_reading
WHERE time >= '2025-01-14 23:00:00' AND time < '2025-01-15 00:00:00'
