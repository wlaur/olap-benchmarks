SELECT *
FROM order_events
WHERE event_created < '2024-09-01'
  AND order_id = '333'
ORDER BY
    event_created DESC,
    counter DESC
LIMIT 1;
