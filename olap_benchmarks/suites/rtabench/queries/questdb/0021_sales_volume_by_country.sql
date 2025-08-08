SELECT
    c.country,
    sum(oi.amount * p.price) AS total_sales
FROM
    customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
WHERE
    o.created_at >= '2024-01-01T00:00:00'
    AND o.created_at < '2024-02-01T00:00:00'
GROUP BY
    c.country
ORDER BY
    total_sales DESC;
