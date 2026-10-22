SELECT
  did AS user_id,
  fromUnixTimestamp64Milli(toInt64(sign(toInt64(min(time_us_text))) * intDiv(abs(toInt128(toInt64(min(time_us_text)))), 1000)), 'UTC') AS first_post_ts
FROM json_prepared
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
  AND (collection) = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY toInt64(min(time_us_text)) ASC, user_id ASC NULLS LAST
LIMIT 3;
