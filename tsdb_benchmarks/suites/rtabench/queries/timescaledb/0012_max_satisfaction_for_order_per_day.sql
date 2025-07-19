SELECT
    time_bucket('1 day', event_created) as day,
    max(satisfaction)
FROM
    order_events
WHERE
    order_id = 700
GROUP BY
    day
ORDER BY
    day desc;
