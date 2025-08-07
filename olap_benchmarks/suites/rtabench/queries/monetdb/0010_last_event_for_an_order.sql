-- pymonetdb does not fetch JSON columns correctly, cast to string to match e.g. duckdb
SELECT
  order_id,
  counter,
  event_created,
  event_type,
  satisfaction,
  processor,
  backup_processor,
  cast(event_payload as text) as event_payload
FROM
  order_events
WHERE
  event_created < '2024-09-01'
  AND order_id = 333
ORDER BY
  event_created DESC
LIMIT
  1;
