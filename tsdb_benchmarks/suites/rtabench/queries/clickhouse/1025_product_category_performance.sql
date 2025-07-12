SELECT
    category,
    sum(volume)
FROM
    top_sales_volume_product
WHERE
    event_created >= '2024-01-01' and event_created < '2024-01-07'
GROUP BY
    category
ORDER BY sum(volume) DESC;
