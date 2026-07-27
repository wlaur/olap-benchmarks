SELECT
  data.did::String AS user_id,
  fromUnixTimestamp64Milli(intDiv(min(data.time_us), 1000), 'UTC') AS first_post_ts
FROM bluesky
WHERE
  data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY min(data.time_us) ASC
LIMIT 3;
