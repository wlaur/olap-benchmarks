
SELECT
    sum(CASE WHEN years_diff(now(), c.birthday) >= 18 AND years_diff(now(), c.birthday) < 26 THEN p.price * oi.amount END) AS `18-25`,
    sum(CASE WHEN years_diff(now(), c.birthday) >= 26 AND years_diff(now(), c.birthday) < 36 THEN p.price * oi.amount END) AS `26-35`,
    sum(CASE WHEN years_diff(now(), c.birthday) >= 36 AND years_diff(now(), c.birthday) < 51 THEN p.price * oi.amount END) AS `36-50`,
    sum(CASE WHEN years_diff(now(), c.birthday) >= 51 AND years_diff(now(), c.birthday) < 66 THEN p.price * oi.amount END) AS `51-65`,
    sum(CASE WHEN years_diff(now(), c.birthday) >= 66 THEN p.price * oi.amount END) AS `66+`
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN orders o ON o.order_id = oi.order_id AND o.created_at > '2024-01-01' AND o.created_at < '2024-01-07'
    INNER JOIN customers c ON c.customer_id = o.customer_id
;
