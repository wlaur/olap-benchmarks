SELECT
    *
FROM
    order_events
WHERE
    backup_processor != ''
    and backup_processor is not null
ORDER BY
    event_created,
    order_id,
    counter
LIMIT
    10;
