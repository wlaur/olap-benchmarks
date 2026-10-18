WITH hourly AS (
    SELECT hour, terminal, sum(event_count) AS event_count,
           uniqExactMerge(unique_orders) AS unique_orders
    FROM order_events_per_terminal_per_hour
    WHERE terminal IN ('Berlin', 'Hamburg', 'Munich')
      AND hour >= '2024-01-01' AND hour < '2024-02-01'
    GROUP BY hour, terminal
)
SELECT hour, terminal, event_count, unique_orders,
       avg(event_count) OVER (
           PARTITION BY terminal ORDER BY hour ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
       ) AS moving_avg_events
FROM hourly
ORDER BY terminal, hour;
