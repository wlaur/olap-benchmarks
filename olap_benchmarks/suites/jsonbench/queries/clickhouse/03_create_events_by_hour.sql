SELECT
  data.commit.collection AS event,
  toHour(fromUnixTimestamp64Micro(data.time_us)) AS hour_of_day,
  count() AS count
FROM bluesky
WHERE
  data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection IN [
    'app.bsky.feed.post',
    'app.bsky.feed.repost',
    'app.bsky.feed.like'
  ]
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event;
