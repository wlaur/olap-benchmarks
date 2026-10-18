SELECT p.product_id, p.name AS product_name, sum(oi.amount * p.price) AS volume
FROM products p
JOIN order_items oi USING (product_id)
JOIN order_events oe USING (order_id)
WHERE oe.event_created >= '2024-07-01' AND oe.event_created < '2025-01-01'
  AND json_extract_string(oe.event_payload, '$.terminal') = 'Berlin'
GROUP BY p.product_id, p.name
ORDER BY volume DESC, p.product_id
LIMIT 10;
