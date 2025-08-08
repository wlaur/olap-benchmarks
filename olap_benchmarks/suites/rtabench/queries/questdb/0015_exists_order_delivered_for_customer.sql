SELECT
    count() > 0 exists_flag
FROM
    order_events
    JOIN orders ON order_events.order_id = orders.order_id
WHERE
    customer_id = 124
    AND event_type = 'Delivered';
