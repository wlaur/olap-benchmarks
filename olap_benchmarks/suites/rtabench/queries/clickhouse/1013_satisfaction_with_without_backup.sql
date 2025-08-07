SELECT
    month,
    count(count_with_backup) as count_with_backup,
    count(count_without_backup) as count_without_backup,
    avg(avg_satisfaction_with_backup) as avg_satisfaction_with_backup,
    avg(avg_satisfaction_without_backup) as avg_satisfaction_without_backup
FROM
    order_events_q13
WHERE
    order_id = 111
GROUP BY
    month
ORDER BY
    month desc
