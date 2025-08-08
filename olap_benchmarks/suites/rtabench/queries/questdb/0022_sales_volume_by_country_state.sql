-- Grand total (equivalent to empty grouping set ())
SELECT
    NULL as country,
    NULL as state,
    sum(amount * price) as total_amount
FROM
    customers c
    INNER JOIN orders o ON o.customer_id = c.customer_id
    AND o.created_at >= '2024-12-24'
    AND o.created_at < '2025-01-01'
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
UNION
ALL -- Country totals (equivalent to grouping set (country))
SELECT
    c.country,
    NULL as state,
    sum(amount * price) as total_amount
FROM
    customers c
    INNER JOIN orders o ON o.customer_id = c.customer_id
    AND o.created_at >= '2024-12-24'
    AND o.created_at < '2025-01-01'
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
GROUP BY
    c.country
UNION
ALL -- Country and state totals (equivalent to grouping set (country, state))
SELECT
    c.country,
    c.state,
    sum(amount * price) as total_amount
FROM
    customers c
    INNER JOIN orders o ON o.customer_id = c.customer_id
    AND o.created_at >= '2024-12-24'
    AND o.created_at < '2025-01-01'
    INNER JOIN order_items oi ON o.order_id = oi.order_id
    INNER JOIN products p ON oi.product_id = p.product_id
GROUP BY
    c.country,
    c.state
ORDER BY
    total_amount,
    country,
    state;
