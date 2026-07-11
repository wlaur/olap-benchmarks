SELECT
  j ->> '$.did' AS user_id,
  CAST(max(j ->> '$.time_us') AS BIGINT) // 1000
    - CAST(min(j ->> '$.time_us') AS BIGINT) // 1000 AS activity_span
FROM bluesky
WHERE
  (j ->> '$.kind') = 'commit'
  AND (j ->> '$.commit.operation') = 'create'
  AND (j ->> '$.commit.collection') = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;
