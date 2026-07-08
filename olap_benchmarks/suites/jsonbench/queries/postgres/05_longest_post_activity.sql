SELECT
  data ->> 'did' AS user_id,
  EXTRACT(
    EPOCH FROM (
      max(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data ->> 'time_us')::BIGINT)
      - min(TIMESTAMP WITH TIME ZONE 'epoch' + INTERVAL '1 microsecond' * (data ->> 'time_us')::BIGINT)
    )
  ) * 1000 AS activity_span
FROM bluesky
WHERE
  data ->> 'kind' = 'commit'
  AND data -> 'commit' ->> 'operation' = 'create'
  AND data -> 'commit' ->> 'collection' = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;
