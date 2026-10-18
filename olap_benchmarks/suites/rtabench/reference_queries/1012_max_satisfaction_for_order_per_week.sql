SELECT date_trunc('week', event_created) AS week, max(satisfaction) AS satisfaction
FROM order_events
WHERE order_id = 700
GROUP BY week
ORDER BY week DESC;
