SELECT day,
       count
FROM cagg_order_events_0004
WHERE day > '2024-05-01' and day < '2024-06-01'
ORDER BY count desc
limit 20;
