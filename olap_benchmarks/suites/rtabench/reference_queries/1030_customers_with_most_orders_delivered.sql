SELECT c.customer_id, c.name AS customer_name, count(o.order_id) AS customer_orders
FROM customers c
JOIN orders o USING (customer_id)
JOIN order_events oe USING (order_id)
WHERE oe.event_type = 'Delivered'
  AND oe.event_created >= '2024-01-01' AND oe.event_created < '2024-07-01'
GROUP BY c.customer_id, c.name
ORDER BY customer_orders DESC, c.customer_id
LIMIT 10;
