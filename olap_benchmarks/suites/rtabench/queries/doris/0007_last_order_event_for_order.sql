SELECT order_id, event_created, event_type FROM (
    SELECT
        oe.order_id,
        oe.event_created,
        oe.event_type,
        row_number() OVER (PARTITION BY oe.order_id ORDER BY oe.event_created DESC) AS rn
    FROM order_events oe
    JOIN orders ON orders.order_id = oe.order_id
    WHERE orders.order_id = 2344
) t
WHERE rn = 1
ORDER BY order_id ASC;
