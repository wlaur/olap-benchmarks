SELECT p.category, sum(oi.amount * p.price) AS volume
FROM products p
JOIN order_items oi USING (product_id)
JOIN order_events oe USING (order_id)
WHERE oe.event_type = 'Delivered'
  AND oe.event_created >= '2024-01-01' AND oe.event_created < '2024-01-08'
GROUP BY p.category
ORDER BY volume DESC, p.category;
