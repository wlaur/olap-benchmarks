SELECT p.product_id, p.name AS product_name, sum(oi.amount) AS amount
FROM products p
JOIN order_items oi USING (product_id)
JOIN order_events oe USING (order_id)
WHERE oe.event_type = 'Delivered'
  AND oe.event_created >= '2024-01-01' AND oe.event_created < '2024-02-01'
GROUP BY p.product_id, p.name
ORDER BY amount DESC, p.product_id
LIMIT 10;
