SELECT
    p.product_id,
    p.name,
    sum(oi.amount * p.price) AS total_sales
FROM
    products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON o.order_id = oi.order_id
    AND o.created_at > '2024-12-24T00:00:00'
    AND o.created_at < '2025-01-01T00:00:00'
    JOIN customers c ON c.customer_id = o.customer_id
WHERE
    datediff('y', c.birthday, now()) BETWEEN 18
    AND 25
GROUP BY
    p.product_id,
    p.name
ORDER BY
    total_sales DESC
LIMIT
    10;
