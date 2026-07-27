SELECT date_trunc('month', event_created) as month,
       sum(CASE WHEN backup_processor <> '' THEN 1 ELSE 0 END) as count_with_backup,
       sum(CASE WHEN backup_processor is null THEN 1 ELSE 0 END) as count_without_backup,
       avg(CASE WHEN backup_processor <> '' THEN satisfaction END) as avg_satisfaction_with_backup,
       avg(CASE WHEN backup_processor is null THEN satisfaction END) as avg_satisfaction_without_backup
FROM order_events
WHERE order_id = 112
GROUP BY month
ORDER BY month desc
