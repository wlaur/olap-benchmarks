SELECT
  collection AS event,
  hour(to_timestamp(time_us / 1000000) AT TIME ZONE 'UTC') AS hour_of_day,
  count(*) AS count
FROM json_prepared
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
  AND (collection) IN (
    'app.bsky.feed.post',
    'app.bsky.feed.repost',
    'app.bsky.feed.like'
  )
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event;
