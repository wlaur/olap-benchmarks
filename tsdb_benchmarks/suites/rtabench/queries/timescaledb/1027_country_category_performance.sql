SELECT
    country,
    product_category,
    sum(revenue)
FROM
    cagg_country_category_monthly_performance
WHERE
    country = 'Switzerland'
    AND event_created >= '2024-01-01'
    AND event_created < '2024-02-01'
GROUP BY
    country,
    product_category
ORDER BY
    sum(revenue) DESC;
