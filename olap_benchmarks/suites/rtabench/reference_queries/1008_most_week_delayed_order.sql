SELECT order_id, count(*) AS count
FROM order_events
WHERE event_created >= '2024-01-29' AND event_created < '2024-02-05'
  AND list_has_all((event_payload -> 'status')::VARCHAR[], ['Delayed', 'Priority'])
GROUP BY order_id
ORDER BY count DESC, order_id
LIMIT 1;
