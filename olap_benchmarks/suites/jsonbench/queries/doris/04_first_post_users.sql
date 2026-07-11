SELECT
  json_extract_string(data, '$.did') AS user_id,
  CAST(
    from_unixtime(floor(min(json_extract_bigint(data, '$.time_us')) / 1000.0) / 1000.0)
    AS DATETIME(3)
  ) AS first_post_ts
FROM bluesky
WHERE
  json_extract_string(data, '$.kind') = 'commit'
  AND json_extract_string(data, '$.commit.operation') = 'create'
  AND json_extract_string(data, '$.commit.collection') = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY min(json_extract_bigint(data, '$.time_us')) ASC
LIMIT 3;
