SELECT * FROM order_events
         WHERE backup_processor <> '' ORDER BY
    event_created,
    order_id,
    counter
LIMIT 10;
