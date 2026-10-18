SELECT date_trunc('month', event_created) AS month,
       count(*) FILTER (WHERE backup_processor <> '') AS count_with_backup,
       count(*) FILTER (WHERE backup_processor IS NULL) AS count_without_backup,
       avg(satisfaction) FILTER (WHERE backup_processor <> '') AS avg_satisfaction_with_backup,
       avg(satisfaction) FILTER (WHERE backup_processor IS NULL) AS avg_satisfaction_without_backup
FROM order_events
WHERE order_id = 111
GROUP BY month
ORDER BY month DESC;
