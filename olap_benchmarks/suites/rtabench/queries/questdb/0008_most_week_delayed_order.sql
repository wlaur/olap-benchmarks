SELECT
  order_id,
  count() AS count
FROM
  order_events
WHERE
  event_created >= '2024-01-29T00:00:00'
  AND event_created < '2024-02-05T00:00:00'
  AND event_payload LIKE '%"Delayed"%'
GROUP BY
  order_id
ORDER BY
  count ASC,
  order_id DESC
LIMIT
  1;
