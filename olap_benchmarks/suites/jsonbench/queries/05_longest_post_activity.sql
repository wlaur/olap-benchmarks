SELECT
  j ->> '$.did' AS user_id,
  date_diff(
    'milliseconds',
    to_timestamp(CAST(min(j ->> '$.time_us') AS BIGINT) / 1000000),
    to_timestamp(CAST(max(j ->> '$.time_us') AS BIGINT) / 1000000)
  ) AS activity_span
FROM bluesky
WHERE
  j ->> '$.kind' = 'commit'
  AND j ->> '$.commit.operation' = 'create'
  AND j ->> '$.commit.collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;
