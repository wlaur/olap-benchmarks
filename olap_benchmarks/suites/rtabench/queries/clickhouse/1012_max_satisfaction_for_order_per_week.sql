SELECT
    week,
    max(satisfaction)
FROM
    order_events_q12
WHERE
    order_id = 700
GROUP BY
    week
ORDER BY
    week desc
