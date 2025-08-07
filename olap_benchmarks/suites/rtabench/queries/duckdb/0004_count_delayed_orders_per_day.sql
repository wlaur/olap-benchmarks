SELECT
  date_trunc('day', event_created) as day,
  count(*) as count
FROM
  order_events
WHERE
  list_has_all(
    (event_payload -> 'status') :: varchar [],
    ['Delayed', 'Priority']
  )
  AND event_created >= '2024-05-01'
  and event_created < '2024-06-01'
GROUP BY
  day
ORDER BY
  count desc,
  day
limit
  20;
