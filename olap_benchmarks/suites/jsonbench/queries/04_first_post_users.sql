SELECT
  j ->> '$.did' AS user_id,
  epoch_ms(CAST(min(j ->> '$.time_us') AS BIGINT) // 1000) AS first_post_ts
FROM bluesky
WHERE
  (j ->> '$.kind') = 'commit'
  AND (j ->> '$.commit.operation') = 'create'
  AND (j ->> '$.commit.collection') = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY CAST(min(j ->> '$.time_us') AS BIGINT) ASC
LIMIT 3;
