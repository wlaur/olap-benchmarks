WITH customer_ages AS (
    SELECT
        c.customer_id,
        -- this age calculation might be incorrect, query output matches DuckDB at least
        EXTRACT(
            YEAR
            FROM
                now()
        ) - EXTRACT(
            YEAR
            FROM
                c.birthday
        ) - CASE
            WHEN (
                EXTRACT(
                    MONTH
                    FROM
                        now()
                ) < EXTRACT(
                    MONTH
                    FROM
                        c.birthday
                )
            )
            OR (
                EXTRACT(
                    MONTH
                    FROM
                        now()
                ) = EXTRACT(
                    MONTH
                    FROM
                        c.birthday
                )
                AND EXTRACT(
                    DAY
                    FROM
                        now()
                ) < EXTRACT(
                    DAY
                    FROM
                        c.birthday
                )
            ) THEN 1
            ELSE 0
        END AS customer_age_years
    FROM
        customers c
)
SELECT
    product_id,
    p.name,
    SUM(oi.amount * p.price) AS total_sales
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN orders o ON o.order_id = oi.order_id
    AND o.created_at > DATE '2024-12-24'
    AND o.created_at < DATE '2025-01-01'
    INNER JOIN customer_ages ca ON ca.customer_id = o.customer_id
    AND ca.customer_age_years >= 18
    AND ca.customer_age_years < 26
GROUP BY
    product_id,
    p.name
ORDER BY
    total_sales DESC
LIMIT
    10;
