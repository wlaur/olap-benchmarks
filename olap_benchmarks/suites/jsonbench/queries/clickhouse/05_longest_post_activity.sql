SELECT
  data.did::String AS user_id,
  intDiv(max(data.time_us), 1000) - intDiv(min(data.time_us), 1000) AS activity_span
FROM bluesky
WHERE
  data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;
