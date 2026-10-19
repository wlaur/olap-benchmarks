SELECT c.country, p.category AS product_category, sum(p.price * oi.amount) AS revenue
FROM products p
JOIN order_items oi USING (product_id)
JOIN orders o USING (order_id)
JOIN customers c USING (customer_id)
JOIN order_events oe USING (order_id)
WHERE oe.event_type = 'Delivered' AND c.country = 'Switzerland'
  AND oe.event_created >= '2024-01-01' AND oe.event_created < '2024-02-01'
GROUP BY c.country, p.category
ORDER BY revenue DESC, p.category;
