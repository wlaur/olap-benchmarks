WITH hourly_stats AS (
  SELECT
    date_trunc('hour', event_created) :: timestamp AS hour,
    json_extract(event_payload, '$.terminal') :: varchar AS terminal,
    count(*) AS event_count
  FROM
    order_events
  WHERE
    event_created >= '2024-01-01T00:00:00'
    AND event_created < '2024-02-01T00:00:00'
    AND event_type IN ('Created', 'Departed', 'Delivered')
  GROUP BY
    hour,
    terminal
)
SELECT
  hour,
  terminal,
  event_count,
  AVG(event_count) OVER (
    PARTITION BY terminal
    ORDER BY
      hour ROWS BETWEEN 3 PRECEDING
      AND CURRENT ROW
  ) AS moving_avg_events
FROM
  hourly_stats
WHERE
  terminal IN ('Berlin', 'Hamburg', 'Munich')
ORDER BY
  terminal,
  hour;
