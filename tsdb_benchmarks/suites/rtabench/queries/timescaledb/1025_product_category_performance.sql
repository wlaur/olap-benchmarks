SELECT
    category,
    volume
FROM
    cagg_top_sales_volume_category_weekly
WHERE
    event_created = '2024-01-01'
ORDER BY volume DESC;
