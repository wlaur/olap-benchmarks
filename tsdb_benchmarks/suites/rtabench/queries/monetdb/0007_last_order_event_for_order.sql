SELECT order_id, event_created, event_type
FROM (
    SELECT
        oe.order_id,
        event_created,
        event_type,
        ROW_NUMBER() OVER (
            PARTITION BY oe.order_id
            ORDER BY event_created DESC
        ) AS rn
    FROM order_events oe
    JOIN orders ON orders.order_id = oe.order_id
    WHERE orders.order_id = 2344
) AS ranked
WHERE rn = 1;
