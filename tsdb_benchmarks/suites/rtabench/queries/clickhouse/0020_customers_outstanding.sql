SELECT
    c.customer_id,
    c.name
FROM
    customers c
WHERE
    c.customer_id IN (
        SELECT
            customer_id
        FROM
            orders o
        WHERE
            o.created_at >= '2024-12-25' AND o.created_at < '2025-01-01' AND
            o.order_id NOT IN (
                SELECT
                    order_id
                FROM
                    order_events oe
                WHERE
                    oe.event_type = 'Delivered'
            )
    )
ORDER BY c.customer_id;
