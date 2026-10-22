SELECT toStartOfDay(time) AS bucket, region, COUNT(*) AS samples,
       avgOrNull(cpu_user) AS avg_cpu, avgOrNull(memory_used) AS avg_memory
FROM telemetry_reading
GROUP BY bucket, region
ORDER BY bucket, region
