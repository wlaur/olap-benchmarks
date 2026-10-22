SELECT DATE_TRUNC('day', time) AS bucket, region, COUNT(*) AS samples,
       AVG(cpu_user) AS avg_cpu, AVG(memory_used) AS avg_memory
FROM telemetry_reading
GROUP BY bucket, region
ORDER BY bucket, region
