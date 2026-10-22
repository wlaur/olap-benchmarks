SELECT
  did AS user_id,
  CAST(max(time_us_text) AS BIGINT) // 1000
    - CAST(min(time_us_text) AS BIGINT) // 1000 AS activity_span
FROM json_prepared
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
  AND (collection) = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC, user_id ASC NULLS LAST
LIMIT 3;
