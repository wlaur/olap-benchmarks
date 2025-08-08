SELECT
  date_trunc('day', event_created) :: timestamp AS day,
  count() AS count
FROM
  order_events
WHERE
  event_created >= '2024-05-01T00:00:00'
  AND event_created < '2024-06-01T00:00:00'
  AND event_payload LIKE '%"Delayed"%'
  AND event_payload LIKE '%"Priority"%'
GROUP BY
  day
ORDER BY
  count DESC,
  day
LIMIT
  20;
