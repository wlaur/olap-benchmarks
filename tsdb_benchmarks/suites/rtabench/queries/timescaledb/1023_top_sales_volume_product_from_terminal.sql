SELECT
    product_id,
    product_name,
    volume
FROM
    cagg_top_selling_product_semester
WHERE
    event_created = '2024-07-01'
    and terminal = 'Berlin'
ORDER BY
    volume DESC
LIMIT
    10;
