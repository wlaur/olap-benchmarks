SELECT
    c.customer_id,
    c.name,
    sum(oi.amount * p.price)
FROM
    customers c,
    orders o,
    order_items oi,
    products p
WHERE
    c.customer_id = o.customer_id
    AND o.order_id = oi.order_id
    AND oi.product_id = p.product_id
    AND o.created_at >= '2024-01-01' and o.created_at < '2024-02-01'
GROUP BY
    c.customer_id, c.name
ORDER BY
    sum(oi.amount * p.price) DESC
LIMIT 10;
