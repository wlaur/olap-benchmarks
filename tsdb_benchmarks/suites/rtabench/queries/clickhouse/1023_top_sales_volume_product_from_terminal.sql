SELECT
    product_id,
    product_name,
    sum(volume)
FROM
	top_sales_volume_product
WHERE
    event_created >= '2024-07-01' AND event_created < '2024-12-25'
    and terminal = 'Berlin'
GROUP BY
    product_id, product_name
ORDER BY
    sum(volume) DESC
LIMIT 10;
