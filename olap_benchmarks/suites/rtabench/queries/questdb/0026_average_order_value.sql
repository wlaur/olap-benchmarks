SELECT
    sum(oi.amount * p.price) / count(DISTINCT o.order_id) AS avg_order_value
FROM
    orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
WHERE
    o.created_at >= '2024-01-01T00:00:00'
    AND o.created_at < '2024-01-07T00:00:00';
