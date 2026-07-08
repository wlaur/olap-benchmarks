SELECT
  data ->> 'did' AS user_id,
  min(
    (
      TIMESTAMP WITH TIME ZONE 'epoch'
      + INTERVAL '1 microsecond' * (data ->> 'time_us')::BIGINT
    ) AT TIME ZONE 'UTC'
  )::timestamp(3) AS first_post_ts
FROM bluesky
WHERE
  data ->> 'kind' = 'commit'
  AND data -> 'commit' ->> 'operation' = 'create'
  AND data -> 'commit' ->> 'collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_ts ASC
LIMIT 3;
