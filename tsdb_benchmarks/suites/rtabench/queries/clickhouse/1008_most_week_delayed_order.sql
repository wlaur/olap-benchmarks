SELECT
    day,
    order_id,
    sum(delayed_orders) as delayed_orders
FROM
    order_events_q8
WHERE
    day >= '2024-02-01'
    and day < '2024-02-07'
GROUP BY
    day,
    order_id
ORDER BY
    delayed_orders desc
limit
    1;
