SELECT
  get_json_string(data, 'commit.collection') AS event,
  hour_from_unixtime(get_json_int(data, 'time_us') / 1000000) AS hour_of_day,
  count() AS count
FROM bluesky
WHERE
  get_json_string(data, 'kind') = 'commit'
  AND get_json_string(data, 'commit.operation') = 'create'
  AND array_contains(
    ['app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like'],
    get_json_string(data, 'commit.collection')
  )
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event;
