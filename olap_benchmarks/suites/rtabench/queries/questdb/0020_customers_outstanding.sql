-- Step 1: Precompute delivered customers as a CTE
WITH delivered_customers AS (
    SELECT
        DISTINCT o.customer_id
    FROM
        orders o
        JOIN order_events oe ON oe.order_id = o.order_id
    WHERE
        o.created_at >= '2024-12-25T00:00:00'
        AND o.created_at < '2025-01-01T00:00:00'
        AND oe.event_type = 'Delivered'
) -- Step 2: Use LEFT JOIN + IS NULL to exclude them
SELECT
    DISTINCT c.customer_id,
    c.name
FROM
    customers c
    JOIN orders o ON c.customer_id = o.customer_id
    LEFT JOIN delivered_customers dc ON dc.customer_id = c.customer_id
    LEFT JOIN order_events oe ON oe.order_id = o.order_id
    AND oe.event_type = 'Delivered'
WHERE
    o.created_at >= '2024-12-25T00:00:00'
    AND o.created_at < '2025-01-01T00:00:00'
    AND oe.order_id IS NULL
    AND dc.customer_id IS NULL
ORDER BY
    c.customer_id;
