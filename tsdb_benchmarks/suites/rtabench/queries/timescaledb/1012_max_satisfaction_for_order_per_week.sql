SELECT week,
       satisfaction
FROM cagg_order_events_q12
WHERE order_id = 700
ORDER BY week desc;
