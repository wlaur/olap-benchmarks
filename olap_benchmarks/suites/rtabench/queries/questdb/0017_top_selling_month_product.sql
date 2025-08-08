SELECT
    p.product_id,
    p.name,
    sum(oi.amount) AS total_amount
FROM
    products p
    INNER JOIN order_items oi ON p.product_id = oi.product_id
    INNER JOIN order_events oe ON oi.order_id = oe.order_id
WHERE
    oe.event_created >= '2024-01-01T00:00:00'
    AND oe.event_created < '2024-02-01T00:00:00'
    AND oe.event_type = 'Delivered'
GROUP BY
    p.product_id,
    p.name
ORDER BY
    total_amount,
    p.name DESC
LIMIT
    10;
