SELECT
    name,
    stock,
    total_ordered
FROM
    (
        SELECT
            p.name,
            p.stock,
            sum(oi.amount) AS total_ordered
        FROM
            orders o
            INNER JOIN order_items oi ON o.order_id = oi.order_id
            INNER JOIN products p ON oi.product_id = p.product_id
            LEFT JOIN order_events oe ON oe.order_id = o.order_id
            AND oe.event_type = 'Shipped'
        WHERE
            o.created_at >= '2024-12-01T00:00:00'
            AND o.created_at < '2024-12-07T00:00:00'
            AND oe.order_id IS NULL
        GROUP BY
            p.name,
            p.stock
    )
WHERE
    total_ordered < stock;
