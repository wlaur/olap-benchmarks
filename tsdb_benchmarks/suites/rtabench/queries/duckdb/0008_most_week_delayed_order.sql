SELECT
  order_id,
  count(*) as count
FROM
  order_events
WHERE
  event_created >= '2024-01-29'
  and event_created < '2024-02-05'
  AND list_has_all(
    (event_payload -> 'status') :: varchar [],
    ['Delayed']
  )
GROUP BY
  order_id
ORDER BY
  count,
  order_id desc
limit
  1;
