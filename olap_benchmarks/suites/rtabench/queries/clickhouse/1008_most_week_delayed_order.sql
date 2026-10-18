SELECT order_id, sum(delayed_orders) AS count
FROM order_events_q8
WHERE day >= '2024-01-29' AND day < '2024-02-05'
GROUP BY order_id
ORDER BY count DESC, order_id
LIMIT 1;
