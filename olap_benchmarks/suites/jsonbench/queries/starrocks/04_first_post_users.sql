SELECT
  get_json_string(data, 'did') AS user_id,
  to_datetime(floor(min(get_json_int(data, 'time_us')) / 1000.0) * 1000, 6) AS first_post_ts
FROM bluesky
WHERE
  get_json_string(data, 'kind') = 'commit'
  AND get_json_string(data, 'commit.operation') = 'create'
  AND get_json_string(data, 'commit.collection') = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY min(get_json_int(data, 'time_us')) ASC
LIMIT 3;
