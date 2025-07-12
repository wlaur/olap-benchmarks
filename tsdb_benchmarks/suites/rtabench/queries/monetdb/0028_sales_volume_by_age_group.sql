WITH customer_ages AS (
    SELECT
        c.customer_id,
        -- this age calculation might be incorrect, query output matches DuckDB at least
        (
            (EXTRACT(YEAR FROM now()) * 12 + EXTRACT(MONTH FROM now()))
          - (EXTRACT(YEAR FROM c.birthday) * 12 + EXTRACT(MONTH FROM c.birthday))
        ) / 12.0 AS customer_age_years
    FROM
        customers c
)
SELECT
    SUM(CASE WHEN ca.customer_age_years >= 18 AND ca.customer_age_years < 26 THEN p.price * oi.amount ELSE NULL END) AS "18-25",
    SUM(CASE WHEN ca.customer_age_years >= 26 AND ca.customer_age_years < 36 THEN p.price * oi.amount ELSE NULL END) AS "26-35",
    SUM(CASE WHEN ca.customer_age_years >= 36 AND ca.customer_age_years < 51 THEN p.price * oi.amount ELSE NULL END) AS "36-50",
    SUM(CASE WHEN ca.customer_age_years >= 51 AND ca.customer_age_years < 66 THEN p.price * oi.amount ELSE NULL END) AS "51-65",
    SUM(CASE WHEN ca.customer_age_years >= 66 THEN p.price * oi.amount ELSE NULL END) AS "66+"
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN orders o
        ON o.order_id = oi.order_id
       AND o.created_at > DATE '2024-01-01'
       AND o.created_at < DATE '2024-01-07'
    INNER JOIN customer_ages ca ON ca.customer_id = o.customer_id;
