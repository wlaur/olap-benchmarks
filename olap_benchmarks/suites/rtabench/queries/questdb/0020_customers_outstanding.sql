SELECT
    DISTINCT c.customer_id,
    c.name
FROM
    customers AS c
    JOIN orders AS o ON o.customer_id = c.customer_id
    AND o.created_at >= '2024-12-25'
    AND o.created_at < '2025-01-01'
    LEFT JOIN order_events AS oe ON oe.order_id = o.order_id
    AND oe.event_type = 'Delivered'
WHERE
    oe.order_id IS NULL
ORDER BY
    c.customer_id;
