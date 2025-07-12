SELECT
  date_trunc('month', event_created) AS "month",

  count(CASE WHEN backup_processor <> '' THEN 1 ELSE NULL END) AS count_with_backup,
  count(CASE WHEN backup_processor IS NULL THEN 1 ELSE NULL END) AS count_without_backup,

  avg(CASE WHEN backup_processor <> '' THEN satisfaction ELSE NULL END) AS avg_satisfaction_with_backup,
  avg(CASE WHEN backup_processor IS NULL THEN satisfaction ELSE NULL END) AS avg_satisfaction_without_backup

FROM order_events
WHERE order_id = 112
GROUP BY "month"
ORDER BY "month" DESC;
