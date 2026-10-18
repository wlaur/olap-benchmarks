SELECT date_trunc('day', event_created) AS day, count(*) AS count
FROM order_events
WHERE event_created >= '2024-05-01' AND event_created < '2024-06-01'
  AND list_has_all((event_payload -> 'status')::VARCHAR[], ['Delayed', 'Priority'])
GROUP BY day
ORDER BY count DESC, day
LIMIT 20;
