SELECT
  data ->> 'did' AS user_id,
  TIMESTAMP 'epoch'
    + INTERVAL '1 millisecond' * (min((data ->> 'time_us')::BIGINT) / 1000) AS first_post_ts
FROM bluesky
WHERE
  data ->> 'kind' = 'commit'
  AND data -> 'commit' ->> 'operation' = 'create'
  AND data -> 'commit' ->> 'collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY min((data ->> 'time_us')::BIGINT) ASC
LIMIT 3;
