SELECT
    date_trunc('day', event_created) :: timestamp AS day,
    count(*) AS event_count
FROM
    order_events
WHERE
    event_created >= '2024-04-01T00:00:00'
    AND event_created < '2024-05-01T00:00:00'
    AND event_type = 'Departed'
    AND json_extract(event_payload, '$.terminal') = 'Berlin'
GROUP BY
    day
ORDER BY
    event_count DESC,
    day;
