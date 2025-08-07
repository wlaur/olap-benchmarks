SELECT
  hour,
  terminal,
  event_count,
  unique_orders,
  avg(event_count) OVER (
    PARTITION BY terminal
    ORDER BY
      hour ROWS BETWEEN 3 PRECEDING
      AND CURRENT ROW
  ) as moving_avg_events
FROM
  cagg_order_events_per_terminal_per_hour
WHERE
  terminal IN ('Berlin', 'Hamburg', 'Munich')
  AND hour >= '2024-01-01'
  and hour < '2024-02-01'
ORDER BY
  terminal,
  hour;
