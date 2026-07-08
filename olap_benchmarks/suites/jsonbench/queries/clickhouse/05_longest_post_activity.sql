SELECT
  data.did::String AS user_id,
  date_diff(
    'milliseconds',
    min(fromUnixTimestamp64Micro(data.time_us)),
    max(fromUnixTimestamp64Micro(data.time_us))
  ) AS activity_span
FROM bluesky
WHERE
  data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;
