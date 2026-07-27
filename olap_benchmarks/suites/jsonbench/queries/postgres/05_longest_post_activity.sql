SELECT
  data ->> 'did' AS user_id,
  max((data ->> 'time_us')::BIGINT) / 1000 - min((data ->> 'time_us')::BIGINT) / 1000 AS activity_span
FROM bluesky
WHERE
  data ->> 'kind' = 'commit'
  AND data -> 'commit' ->> 'operation' = 'create'
  AND data -> 'commit' ->> 'collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;
