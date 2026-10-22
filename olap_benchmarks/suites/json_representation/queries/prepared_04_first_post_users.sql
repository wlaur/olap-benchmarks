SELECT
  did AS user_id,
  epoch_ms(CAST(min(time_us_text) AS BIGINT) // 1000) AS first_post_ts
FROM json_prepared
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
  AND (collection) = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY CAST(min(time_us_text) AS BIGINT) ASC, user_id ASC NULLS LAST
LIMIT 3;
