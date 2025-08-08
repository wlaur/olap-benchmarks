SELECT
    c.country,
    p.category,
    sum(p.price * oi.amount) AS total_sales
FROM
    products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON o.order_id = oi.order_id
    JOIN customers c ON c.customer_id = o.customer_id
    JOIN order_events oe ON oe.order_id = o.order_id
WHERE
    c.country = 'Switzerland'
    AND oe.event_created >= '2024-01-01T00:00:00'
    AND oe.event_created < '2024-02-01T00:00:00'
    AND oe.event_type = 'Delivered'
GROUP BY
    c.country,
    p.category
ORDER BY
    c.country,
    p.category;
