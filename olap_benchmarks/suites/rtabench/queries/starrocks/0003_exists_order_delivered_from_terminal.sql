SELECT
EXISTS (
    SELECT 1
    FROM
        order_events
    JOIN
        orders USING (order_id)
    WHERE
        customer_id = 124
        AND event_type='Delivered'
        AND get_json_string(event_payload, '$.terminal') = 'London'
        AND event_created >= '2024-03-01' and event_created < '2024-04-01'
);
