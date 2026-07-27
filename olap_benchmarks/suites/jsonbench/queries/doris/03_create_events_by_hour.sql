SELECT
  json_extract_string(data, '$.commit.collection') AS event,
  hour(from_unixtime(json_extract_bigint(data, '$.time_us') / 1000000.0)) AS hour_of_day,
  count(*) AS count
FROM bluesky
WHERE
  json_extract_string(data, '$.kind') = 'commit'
  AND json_extract_string(data, '$.commit.operation') = 'create'
  AND json_extract_string(data, '$.commit.collection') IN (
    'app.bsky.feed.post',
    'app.bsky.feed.repost',
    'app.bsky.feed.like'
  )
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event;
