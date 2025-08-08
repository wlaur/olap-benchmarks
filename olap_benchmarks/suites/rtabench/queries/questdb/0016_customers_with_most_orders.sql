SELECT
    c.customer_id,
    c.name,
    count() AS order_count
FROM
    customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
WHERE
    o.created_at >= '2024-01-01T00:00:00'
    AND o.created_at < '2024-07-01T00:00:00'
GROUP BY
    c.customer_id,
    c.name
ORDER BY
    order_count DESC,
    c.name
LIMIT
    10;
