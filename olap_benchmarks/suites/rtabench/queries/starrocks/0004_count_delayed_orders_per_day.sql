SELECT date_trunc('day', event_created) as day,
       count(*)                            as count
FROM order_events
WHERE event_created >= '2024-05-01' and event_created < '2024-06-01'
  AND get_json_string(event_payload, '$.status') LIKE '%"Delayed"%' AND get_json_string(event_payload, '$.status') LIKE '%"Priority"%'
GROUP BY day
ORDER BY count desc, day
limit 20;
