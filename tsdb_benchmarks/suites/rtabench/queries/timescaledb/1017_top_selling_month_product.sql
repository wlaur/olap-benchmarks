SELECT
    product_id,
    product_name,
    sum(amount)
FROM
	cagg_top_selling_month_product_q17
WHERE
    event_created >= '2024-01-01' and event_created < '2024-02-01'
GROUP BY
    product_id, product_name
ORDER BY
    sum(amount) DESC
LIMIT 10;
