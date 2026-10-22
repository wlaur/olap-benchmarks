SELECT
  collection AS event,
  toInt64(toHour(fromUnixTimestamp64Micro(time_us, 'UTC'))) AS hour_of_day,
  toInt64(count()) AS count
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
