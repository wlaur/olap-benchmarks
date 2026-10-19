WITH hourly AS (
    SELECT date_trunc('hour', event_created) AS hour,
           json_extract_string(event_payload, '$.terminal') AS terminal,
           count(*) AS event_count,
           count(DISTINCT json_extract_string(event_payload, '$.order_id')) AS unique_orders
    FROM order_events
    WHERE event_type IN ('Created', 'Departed', 'Delivered')
      AND event_created >= '2024-01-01' AND event_created < '2024-02-01'
    GROUP BY hour, terminal
)
SELECT hour, terminal, event_count, unique_orders,
       avg(event_count) OVER (PARTITION BY terminal ORDER BY hour ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS moving_avg_events
FROM hourly
WHERE terminal IN ('Berlin', 'Hamburg', 'Munich')
ORDER BY terminal, hour;
