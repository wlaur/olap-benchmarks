SELECT
  json_extract_string(data, '$.did') AS user_id,
  CAST(floor(max(json_extract_bigint(data, '$.time_us')) / 1000.0) AS BIGINT)
    - CAST(floor(min(json_extract_bigint(data, '$.time_us')) / 1000.0) AS BIGINT) AS activity_span
FROM bluesky
WHERE
  json_extract_string(data, '$.kind') = 'commit'
  AND json_extract_string(data, '$.commit.operation') = 'create'
  AND json_extract_string(data, '$.commit.collection') = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;
