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
);
