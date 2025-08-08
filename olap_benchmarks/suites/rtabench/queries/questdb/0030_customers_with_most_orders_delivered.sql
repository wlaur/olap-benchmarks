SELECT
    c.customer_id,
    c.name,
    count(o.order_id) AS delivered_orders
FROM
    customers c
    JOIN orders o ON o.customer_id = c.customer_id
    JOIN order_events oe ON oe.order_id = o.order_id
WHERE
    oe.event_created >= '2024-01-01T00:00:00'
    AND oe.event_created < '2024-07-01T00:00:00'
    AND oe.event_type = 'Delivered'
GROUP BY
    c.customer_id,
    c.name
ORDER BY
    delivered_orders DESC
LIMIT
    10;
