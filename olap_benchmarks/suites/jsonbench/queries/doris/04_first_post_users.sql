SELECT
  json_extract_string(data, '$.did') AS user_id,
  from_unixtime(min(json_extract_bigint(data, '$.time_us')) / 1000000.0) AS first_post_ts
FROM bluesky
WHERE
  json_extract_string(data, '$.kind') = 'commit'
  AND json_extract_string(data, '$.commit.operation') = 'create'
  AND json_extract_string(data, '$.commit.collection') = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_ts ASC
LIMIT 3;
