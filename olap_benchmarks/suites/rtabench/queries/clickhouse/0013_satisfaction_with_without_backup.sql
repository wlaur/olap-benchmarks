SELECT
    toStartOfMonth(event_created) as month,
    count(*) FILTER (
        WHERE
            backup_processor <> ''
    ) as count_with_backup,
    count(*) FILTER (
        WHERE
            backup_processor = ''
    ) as count_without_backup,
    -- does not read as null
    avg(satisfaction) FILTER (
        WHERE
            backup_processor <> ''
    ) as avg_satisfaction_with_backup,
    avg(satisfaction) FILTER (
        WHERE
            backup_processor = ''
    ) as avg_satisfaction_without_backup
FROM
    order_events
WHERE
    order_id = 112
GROUP BY
    month
ORDER BY
    month desc
