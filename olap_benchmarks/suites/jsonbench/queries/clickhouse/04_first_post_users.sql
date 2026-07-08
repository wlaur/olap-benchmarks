SELECT
  data.did::String AS user_id,
  min(fromUnixTimestamp64Micro(data.time_us)) AS first_post_ts
FROM bluesky
WHERE
  data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_ts ASC
LIMIT 3;
