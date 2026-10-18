SELECT
    customer_id,
    customer_name,
    sum(customer_orders)
FROM
    customers_with_most_orders_delivered
WHERE
    event_created >= '2024-01-01'
    and event_created < '2024-07-01'
GROUP BY
    customer_id,
    customer_name
ORDER BY
    sum(customer_orders) DESC,
    customer_id
LIMIT
    10;
