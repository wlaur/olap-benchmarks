SELECT category, sum(volume)
FROM top_sales_volume_category_weekly
WHERE event_created = '2024-01-01'
GROUP BY category
ORDER BY sum(volume) DESC, category;
