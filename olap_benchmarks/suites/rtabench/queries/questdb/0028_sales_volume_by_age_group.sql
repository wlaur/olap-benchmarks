SELECT
    sum(
        CASE
            WHEN datediff('y', c.birthday, now()) BETWEEN 18
            AND 25 THEN p.price * oi.amount
            ELSE 0
        END
    ) AS "18_25",
    sum(
        CASE
            WHEN datediff('y', c.birthday, now()) BETWEEN 26
            AND 35 THEN p.price * oi.amount
            ELSE 0
        END
    ) AS "26_35",
    sum(
        CASE
            WHEN datediff('y', c.birthday, now()) BETWEEN 36
            AND 50 THEN p.price * oi.amount
            ELSE 0
        END
    ) AS "36_50",
    sum(
        CASE
            WHEN datediff('y', c.birthday, now()) BETWEEN 51
            AND 65 THEN p.price * oi.amount
            ELSE 0
        END
    ) AS "51_65",
    sum(
        CASE
            WHEN datediff('y', c.birthday, now()) > 65 THEN p.price * oi.amount
            ELSE 0
        END
    ) AS "66_plus"
FROM
    products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON o.order_id = oi.order_id
    AND o.created_at >= '2024-01-01T00:00:00'
    AND o.created_at < '2024-01-07T00:00:00'
    JOIN customers c ON c.customer_id = o.customer_id;
