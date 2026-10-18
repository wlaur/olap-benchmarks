SELECT
    product_id,
    product_name,
    sum(volume)
FROM
    top_sales_volume_product
WHERE
    event_created = '2024-07-01'
    and terminal = 'Berlin'
GROUP BY
    product_id,
    product_name
ORDER BY
    sum(volume) DESC,
    product_id
LIMIT
    10;
