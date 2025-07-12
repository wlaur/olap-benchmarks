WITH shipped AS (
    SELECT
        order_id
    FROM
        order_events
    WHERE
        event_type = 'Shipped'
)
SELECT
    p.name,
    p.stock,
    sum(oi.amount)
FROM
    orders o
    INNER JOIN order_items oi USING (order_id)
    INNER JOIN products p USING (product_id)
WHERE
    o.created_at > '2024-12-01' AND o.created_at < '2024-12-07' AND
    oi.order_id NOT IN (SELECT order_id FROM shipped)
GROUP BY
    p.product_id, p.name, p.stock
HAVING (sum(oi.amount) < p.stock);
