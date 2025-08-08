SELECT
    p.product_id,
    p.name,
    sum(oi.amount * p.price) AS total_sales
FROM
    products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN order_events oe ON oe.order_id = oi.order_id
WHERE
    json_extract(oe.event_payload, '$.terminal') = 'Berlin'
    AND oe.event_created >= '2024-07-01T00:00:00'
    AND oe.event_created < '2025-01-01T00:00:00'
    AND oe.event_type = 'Delivered'
GROUP BY
    p.product_id,
    p.name
ORDER BY
    total_sales DESC
LIMIT
    10;
