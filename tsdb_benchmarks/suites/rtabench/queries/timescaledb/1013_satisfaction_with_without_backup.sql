SELECT month,
       count_with_backup,
       count_without_backup,
       avg_satisfaction_with_backup,
       avg_satisfaction_without_backup
FROM cagg_order_events_q13
WHERE order_id = 111
ORDER BY month desc;
