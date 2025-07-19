SELECT
  toStartOfDay(event_created) as day,
  count(*) as count
FROM
  order_events
WHERE
  day >= '2024-05-01'
  and day < '2024-06-01'
  AND hasAll(
    JSONExtract(event_payload, 'status', 'Array(Nullable(TEXT))'),
    ['Delayed', 'Priority']
  )
GROUP BY
  day
ORDER BY
  count desc,
  day
limit
  20;
