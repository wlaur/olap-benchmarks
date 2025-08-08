SELECT
    p.category,
    sum(p.price * oi.amount) AS total_sales
FROM
    products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON o.order_id = oi.order_id
    JOIN order_events oe ON oe.order_id = o.order_id
WHERE
    oe.event_created >= '2024-01-01T00:00:00'
    AND oe.event_created < '2024-01-07T00:00:00'
    AND oe.event_type = 'Delivered'
GROUP BY
    p.category
ORDER BY
    p.category;
