SELECT
    -- cast before dividing: MonetDB otherwise evaluates the quotient at the numerator's
    -- decimal scale, where the other engines promote to double
    cast(sum(oi.amount * p.price) as double) / count(DISTINCT order_id)
FROM
    orders o
    INNER JOIN order_items oi USING (order_id)
    INNER JOIN products p USING (product_id)
WHERE
    o.created_at >= '2024-01-01'
    AND o.created_at < '2024-01-07';
