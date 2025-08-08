SELECT
    count() > 0 as exists_flag
FROM
    order_events
    JOIN "orders" ON order_events.order_id = orders.order_id
WHERE
    customer_id = 124
    AND event_type = 'Delivered'
    AND json_extract(event_payload, '$.terminal') = 'London'
    AND event_created >= '2024-03-01T00:00:00'
    AND event_created < '2024-04-01T00:00:00';
