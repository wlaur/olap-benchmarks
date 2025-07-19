SELECT
    order_id,
    count
FROM
    cagg_order_events_0008
WHERE
    week >= '2024-01-29'
    and week < '2024-02-05'
ORDER BY
    count desc
LIMIT
    1;
