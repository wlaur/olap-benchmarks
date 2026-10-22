SELECT
  did AS user_id,
  toInt64(sign(toInt64(max(time_us_text))) * intDiv(abs(toInt128(toInt64(max(time_us_text)))), 1000))
    - toInt64(sign(toInt64(min(time_us_text))) * intDiv(abs(toInt128(toInt64(min(time_us_text)))), 1000)) AS activity_span
FROM json_prepared
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
  AND (collection) = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC, user_id ASC NULLS LAST
LIMIT 3;
