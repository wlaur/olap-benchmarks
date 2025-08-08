SELECT
    c.customer_id,
    c.name,
    sum(oi.amount * p.price) AS total_sales
FROM
    customers c
    JOIN orders o ON o.customer_id = c.customer_id
    AND o.created_at > '2024-12-01T00:00:00'
    AND o.created_at < '2024-12-07T00:00:00'
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
GROUP BY
    c.customer_id,
    c.name
ORDER BY
    total_sales DESC
LIMIT
    10;
