SELECT month,
       sum(count_with_backup) AS count_with_backup,
       sum(count_without_backup) AS count_without_backup,
       sum(satisfaction_sum_with_backup) / nullIf(sum(satisfaction_count_with_backup), 0) AS avg_satisfaction_with_backup,
       sum(satisfaction_sum_without_backup) / nullIf(sum(satisfaction_count_without_backup), 0) AS avg_satisfaction_without_backup
FROM order_events_q13
WHERE order_id = 111
GROUP BY month
ORDER BY month DESC;
