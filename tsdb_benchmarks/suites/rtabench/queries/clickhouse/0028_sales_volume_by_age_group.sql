SELECT
    sum(p.price * oi.amount) FILTER (WHERE age('year', c.birthday, now()) >= 18 AND age('year', c.birthday, now()) < 26) AS "18-25",
    sum(p.price * oi.amount) FILTER (WHERE age('year', c.birthday, now()) >= 26 AND age('year', c.birthday, now()) < 36) AS "26-35",
    sum(p.price * oi.amount) FILTER (WHERE age('year', c.birthday, now()) >= 36 AND age('year', c.birthday, now()) < 51) AS "36-50",
    sum(p.price * oi.amount) FILTER (WHERE age('year', c.birthday, now()) >= 51 AND age('year', c.birthday, now()) < 66) AS "51-65",
    sum(p.price * oi.amount) FILTER (WHERE age('year', c.birthday, now()) >= 66) AS "66+"
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN orders o ON o.order_id = oi.order_id AND o.created_at > '2024-01-01' AND o.created_at < '2024-01-07'
    INNER JOIN customers c ON c.customer_id = o.customer_id
;
