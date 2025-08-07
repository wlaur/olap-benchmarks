WITH cte AS (
  SELECT
    order_id,
    event_created,
    CAST(json.filter(event_payload, '$.status') AS TEXT) AS status
  FROM
    order_events
  WHERE
    event_created >= '2024-01-29'
    AND event_created < '2024-02-05'
)
SELECT
  order_id,
  COUNT(*) AS count
FROM
  cte
WHERE
  status LIKE '%"Delayed"%'
GROUP BY
  order_id
ORDER BY
  count ASC,
  order_id DESC
LIMIT
  1;
