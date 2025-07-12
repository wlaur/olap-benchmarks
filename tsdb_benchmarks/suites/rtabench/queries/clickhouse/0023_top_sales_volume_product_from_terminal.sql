SELECT
    product_id,
    p.name,
    sum(oi.amount * p.price)
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN order_events oe ON oe.order_id = oi.order_id
WHERE JSONExtractString(oe.event_payload, 'terminal') = 'Berlin'
    AND oe.event_type = 'Delivered'
    AND oe.event_created >= '2024-07-01' AND oe.event_created < '2025-01-01'
GROUP BY product_id, p.name
ORDER BY sum(oi.amount * p.price) DESC
LIMIT 10;
