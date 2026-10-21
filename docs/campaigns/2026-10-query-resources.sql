-- Run against the selected publication, not a raw database with superseded runs.
-- These are sampled process/container footprints during a query, not its allocations.
WITH resource_samples AS (
    SELECT *,
        max(time) FILTER (WHERE client_mem_mb IS NOT NULL) OVER previous_samples AS rss_interval_start,
        max(time) FILTER (WHERE client_uss_mb IS NOT NULL) OVER previous_samples AS uss_interval_start
    FROM run_metric
    WHERE client_mem_mb IS NOT NULL OR client_uss_mb IS NOT NULL OR server_charge_mb IS NOT NULL
    WINDOW previous_samples AS (
        PARTITION BY run_id ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
), measured_steps AS (
    SELECT r.db, r.suite, r.suite_scale_factor AS sf, s.*
    FROM run r
    JOIN run_step s ON s.run_id = r.id
    WHERE r.operation = 'select'
        AND s.step_type = 'query'
        AND (s.iteration_role = 'measured' OR s.status = 'failed')
        AND s.finished_at IS NOT NULL
)
SELECT
    s.db, s.suite, s.sf, s.run_id, s.query_name, s.iteration,
    s.iteration_role, s.status, s.result_status,
    try_cast(json_extract(s.metadata, '$.duration_ms') AS DOUBLE) AS duration_ms,
    count(m.server_charge_mb) FILTER (WHERE s.db NOT IN ('duckdb', 'polars')) AS server_samples,
    count(m.client_mem_mb) FILTER (WHERE m.rss_interval_start >= s.started_at) AS client_intervals,
    CASE WHEN s.db NOT IN ('duckdb', 'polars')
        THEN max(m.server_anon_mb + m.server_shmem_mb) END AS observed_server_anon_shmem_mb,
    CASE WHEN s.db NOT IN ('duckdb', 'polars')
        THEN max(m.server_file_mb) END AS observed_server_file_including_shmem_mb,
    CASE WHEN s.db NOT IN ('duckdb', 'polars')
        THEN max(m.server_charge_mb) END AS observed_server_charge_mb,
    max(m.client_mem_mb) FILTER (WHERE m.rss_interval_start >= s.started_at) AS observed_client_rss_mb,
    max(m.client_uss_mb) FILTER (WHERE m.uss_interval_start >= s.started_at) AS observed_client_uss_mb
FROM measured_steps s
LEFT JOIN resource_samples m ON m.run_id = s.run_id
    AND m.time >= s.started_at AND m.time < s.finished_at
GROUP BY s.db, s.suite, s.sf, s.run_id, s.query_name, s.iteration,
    s.iteration_role, s.status, s.result_status, duration_ms
ORDER BY s.db, s.suite, s.sf, s.query_name, s.iteration;
