-- Whole-store size includes WAL and excludes client staging; it is not query spill.
-- Peak memory columns are independent maxima and must not be added together.
SELECT
    r.id AS run_id, r.db, r.suite, r.suite_scale_factor AS sf, r.operation, r.status,
    epoch(r.finished_at - r.started_at) AS operation_seconds,
    count(m.server_charge_mb) FILTER (WHERE r.db NOT IN ('duckdb', 'polars')) AS server_samples,
    count(m.client_mem_mb) AS client_samples,
    CASE WHEN r.db NOT IN ('duckdb', 'polars')
        THEN max(m.server_anon_mb + m.server_shmem_mb) END AS peak_server_anon_shmem_mb,
    CASE WHEN r.db NOT IN ('duckdb', 'polars')
        THEN max(m.server_file_mb) END AS peak_server_file_including_shmem_mb,
    CASE WHEN r.db NOT IN ('duckdb', 'polars')
        THEN max(m.server_charge_mb) END AS peak_server_charge_mb,
    max(m.client_mem_mb) AS peak_client_rss_mb,
    max(m.client_uss_mb) AS peak_client_uss_mb,
    count(m.disk_mb) AS disk_samples,
    max(m.disk_mb) AS peak_store_mb,
    arg_max(m.disk_mb, m.time) FILTER (WHERE m.disk_mb IS NOT NULL) AS final_store_mb,
    arg_max(m.disk_data_mb, m.time) FILTER (WHERE m.disk_data_mb IS NOT NULL) AS final_data_mb,
    arg_max(m.disk_wal_mb, m.time) FILTER (WHERE m.disk_wal_mb IS NOT NULL) AS final_wal_mb
FROM run r
LEFT JOIN run_metric m ON m.run_id = r.id
GROUP BY r.id, r.db, r.suite, r.suite_scale_factor, r.operation, r.status, operation_seconds
ORDER BY r.db, r.suite, r.suite_scale_factor, r.operation, r.id;
