SELECT day,
       count(delayed_orders) as delayed_orders
FROM order_events_q4
WHERE day >= '2024-05-01' and day < '2024-06-01'
GROUP BY day
ORDER BY delayed_orders desc
limit 20;
